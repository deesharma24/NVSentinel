# GPU Health Monitor

## Overview

The GPU Health Monitor continuously monitors the health of NVIDIA GPUs in your Kubernetes cluster using DCGM (Data Center GPU Manager). It detects GPU hardware issues like ECC errors, thermal problems, and PCIe failures before they impact your workloads.

Think of it as a heart rate monitor for your GPUs - constantly checking vitals and alerting when something goes wrong.

### Why Do You Need This?

GPU failures are expensive and can cause silent data corruption or job crashes:

- **Early detection**: Catch GPU issues before they crash training jobs
- **Prevent data corruption**: Detect ECC errors that could silently corrupt computation results
- **Avoid cascading failures**: Identify problematic GPUs before they impact multi-node workloads
- **Save time**: Automatically detect issues instead of manual investigation

Without GPU monitoring, failures often go unnoticed until jobs crash or produce incorrect results, wasting valuable compute time and requiring manual troubleshooting.

## How It Works

The GPU Health Monitor runs as a DaemonSet on GPU nodes:

1. Connects to DCGM service running on each node
2. Polls DCGM health checks at regular intervals (default: configured polling interval)
3. Detects GPU health issues (ECC errors, thermal problems, PCIe issues, etc.)
4. Sends health events to Platform Connectors via gRPC
5. Maintains state to avoid duplicate event reporting

DCGM provides comprehensive GPU health monitoring including memory errors, thermal violations, PCIe problems, and more. The monitor translates these DCGM health checks into standardized health events that NVSentinel can act upon.

## Configuration

Configure the GPU Health Monitor through Helm values:

```yaml
gpu-health-monitor:
  enabled: true
  
  dcgm:
    dcgmK8sServiceEnabled: true
    service:
      endpoint: "nvidia-dcgm.gpu-operator.svc"  # DCGM service endpoint
      port: 5555
  
  useHostNetworking: false  # Enable if DCGM requires host network access
  
  verbose: "False"  # Enable debug logging
```

### Configuration Options

- **DCGM Service**: Configure DCGM endpoint and port
- **Host Networking**: Enable if DCGM service requires host network access
- **Polling Interval**: Configure how frequently to check GPU health (set via DCGM configuration)
- **Verbose Logging**: Enable detailed debug output

## What It Monitors

### DCGM Health Watches

The monitor checks multiple GPU health aspects through DCGM. Below are some of the key health watches - this is not an exhaustive list and may evolve over time as DCGM capabilities expand:

**Memory Errors**: Single-bit and double-bit ECC errors
**Thermal Issues**: Temperature violations and throttling events
**PCIe Problems**: PCIe replay errors and link issues
**Power Issues**: Power violations and power capping events
**InfoROM Errors**: GPU InfoROM corruption
**NVLink Errors**: NVLink connectivity and error detection

> **Note**: The specific health watches available depend on your DCGM version and GPU model. NVIDIA regularly adds new health checks and monitoring capabilities to DCGM. Consult your DCGM documentation for the complete list of supported health watches for your environment.

### GPU Count Mismatch Detection (GpuTotalCountMismatch)

#### Why Is This Needed?

DCGM health watches are powerful but they can only monitor GPUs that the NVIDIA driver has already initialized. If a GPU fails during driver initialization at boot, or silently falls off the PCIe bus at runtime without generating an XID error, DCGM simply never sees it -- the GPU vanishes from the software view with no health event generated. This creates a blind spot: a node can be running with fewer GPUs than expected and no existing check raises an alarm.

The GPU Count Mismatch check closes this gap by comparing the **hardware-level** GPU count (from the Linux sysfs PCIe device tree) against the **software-level** GPU count (from DCGM). Because sysfs reflects what the PCIe bus enumerates at boot time, it sees GPUs that the NVIDIA driver may have failed to initialize. Any discrepancy between these two counts indicates a GPU that is physically present on the PCIe bus but not usable by workloads.

#### Coverage

The table below illustrates which failure scenarios are caught by existing syslog-health-check versus this GpuTotalCountMismatch:

| Scenario                                        | Xid79/GpuFallenOffBus       | GpuTotalCountMismatch              |
|-------------------------------------------------|-----------------------------|------------------------------------|
| GPU fails driver init at boot (no XID)          | Miss                        | Catches (sysfs > DCGM)             |
| GPU falls off at runtime (no XID logged)        | Miss                        | Catches (sysfs cached > DCGM live) |
| GPU falls off at runtime (XID 79 logged)        | SysLogsGPUFallenOff catches | Also catches                       |
| GPU physically dead at boot (no PCIe response)  | Miss                        | Miss (invisible to both)           |

#### How It Works

1. **At startup**, the monitor scans sysfs (`/sys/bus/pci/devices/`) for all NVIDIA GPU PCI addresses (vendor `0x10de`, class prefix `0x0302`). This count is cached because sysfs reflects the boot-time PCIe enumeration and does not change during a boot session.
2. **Every polling cycle**, after a successful DCGM health check, the monitor compares the cached sysfs GPU count against the live DCGM GPU count.
3. **If a mismatch is detected** (sysfs count != DCGM count), a fatal `GpuTotalCountMismatch` health event is sent. The event message includes the specific PCI addresses of GPUs that are on the PCIe bus but missing from the driver, making diagnosis straightforward.
4. **If the counts match** (or a previous mismatch resolves), a healthy event is sent to clear any prior mismatch alert.

State-change deduplication ensures that only transitions (healthy to unhealthy or vice versa) generate events, preventing event flooding during repeated polling cycles.

#### Configuration

The GPU count check is enabled by default. It can be disabled via the `gpu_count_check_enabled` parameter if your environment has a known reason for GPU count differences (e.g., intentionally partitioned GPUs).

## Key Features

### DCGM Integration
Leverages NVIDIA's Data Center GPU Manager for comprehensive GPU health monitoring with proven reliability.

### GPU Count Mismatch Detection
Compares hardware-level PCIe GPU enumeration (sysfs) against DCGM's software-visible GPU count to catch GPUs that fail driver initialization or silently drop off the bus -- scenarios that DCGM health watches alone cannot detect.

### Automatic State Management
Maintains entity-level cache to track reported issues and avoid sending duplicate events within a single boot session.

### Categorized Health Events
Maps DCGM health checks to categorized events (fatal, warning, info) for appropriate response levels.

### GPU Metadata Enrichment
Includes GPU serial numbers, UUIDs, and other metadata in health events for precise identification and tracking.

### Connectivity Monitoring
Detects and reports DCGM connectivity issues to ensure monitoring remains functional.
