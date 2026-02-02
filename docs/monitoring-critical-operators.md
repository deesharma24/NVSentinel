# Monitoring Critical Operators (DaemonSet Pods in gpu-operator & network-operator Namespace)

## Overview
For an NVIDIA GPU cluster to function correctly, critical infrastructure components under **gpu-operator** and **network-operator** namespaces must be healthy. If these operators fail, the underlying hardware cannot be utilized effectively.

NVSentinel provides a built-in mechanism to monitor these operators and report health events when their pods are not running correctly.

## Configuration
To monitor the GPU and Network operators, you must enable the `kubernetes-object-monitor` component and define the monitoring policies in your NVSentinel `values.yaml`.

These policies monitor **DaemonSet pods** in the `gpu-operator` and `network-operator` namespaces. A health event is generated if a pod:
- Has been assigned to a node, AND
- Has **not** reached `Running` or `Succeeded` state within the configured threshold 

This threshold allows sufficient time for normal pod initialization (image pulls, init containers, etc.) while detecting pods that are genuinely stuck in any non-progressing state such as:
- Stuck in init container execution
- `Pending` due to resource constraints
- `CrashLoopBackOff` in containers
- `ImagePullBackOff` errors
- Any other state preventing the pod from becoming healthy

### Owner-Level Tracking (DaemonSet Only)

The policies use **owner-level tracking** (`tracking.level: owner`) for DaemonSet pods. This ensures that when a DaemonSet pod is deleted (e.g., during troubleshooting or node maintenance), the node stays cordoned until:
- A **healthy replacement pod** from the same DaemonSet appears on the node, OR
- The **DaemonSet is deleted**, OR
- The **DaemonSet no longer targets this node** (due to nodeSelector or taint changes)

This prevents premature uncordoning when a pod is deleted but the underlying issue hasn't been resolved.

> **Important:** Owner-level tracking **only applies to DaemonSet-owned pods**. Pods without an owner or with non-DaemonSet owners (ReplicaSet, Deployment, Job, StatefulSet, etc.) are **silently skipped** when `tracking.level: owner` is configured. This is by design because:
> - DaemonSets guarantee one pod per node, so replacements appear on the same node
> - ReplicaSet/Deployment pods can be scheduled on any node, so tracking by owner doesn't make sense
> - Jobs may not create replacement pods at all
>
> **Best Practice:** When using `tracking.level: owner`, include a DaemonSet owner check in your CEL predicate:
> ```cel
> has(resource.metadata.ownerReferences) &&
> resource.metadata.ownerReferences.exists(r, r.kind == 'DaemonSet')
> ```
> This is more efficient (avoids evaluating non-DaemonSet pods) and makes the policy's intent explicit.

Add the following configuration to your `values.yaml`:

```yaml
# 1. Enable the component
global:
  kubernetesObjectMonitor:
    enabled: true

# 2. Configure the policies
kubernetes-object-monitor:
  maxConcurrentReconciles: 1
  resyncPeriod: 5m
  policies:
    # Policy 1: Monitor GPU Operator DaemonSet Pods
    - name: gpu-operator-pods-health
      enabled: true
      resource:
        group: ""
        version: v1
        kind: Pod
      predicate:
        # Trigger event if:
        # 1. Pod is in gpu-operator namespace
        # 2. Pod is owned by a DaemonSet (required for owner-level tracking)
        # 3. Pod has been assigned to a node (nodeName is set)
        # 4. Pod is NOT in Running or Succeeded state
        # 5. Pod has been in this non-healthy state for more than configured threshold
        expression: |
          resource.metadata.namespace == 'gpu-operator' && 
          has(resource.metadata.ownerReferences) &&
          resource.metadata.ownerReferences.exists(r, r.kind == 'DaemonSet') &&
          has(resource.spec.nodeName) && resource.spec.nodeName != "" &&
          resource.status.phase != 'Running' && 
          resource.status.phase != 'Succeeded' &&
          has(resource.status.startTime) &&
          now - timestamp(resource.status.startTime) > duration('30m')
      nodeAssociation:
        expression: resource.spec.nodeName
      # Track by owner (DaemonSet) instead of individual pod name
      # This ensures node stays cordoned when a pod is deleted until a healthy
      # replacement pod is running
      tracking:
        level: owner
      healthEvent:
        componentClass: Software
        isFatal: true
        message: "GPU Operator DaemonSet pod has not reached Running state within 30 minutes"
        recommendedAction: CONTACT_SUPPORT
        errorCode:
          - GPU_OPERATOR_POD_UNHEALTHY

    # Policy 2: Monitor Network Operator DaemonSet Pods
    - name: network-operator-pod-health
      enabled: true
      resource:
        group: ""
        version: v1
        kind: Pod
      predicate:
        # Only match DaemonSet pods - required for owner-level tracking
        expression: |
          resource.metadata.namespace == 'network-operator' && 
          has(resource.metadata.ownerReferences) &&
          resource.metadata.ownerReferences.exists(r, r.kind == 'DaemonSet') &&
          has(resource.spec.nodeName) && resource.spec.nodeName != "" &&
          resource.status.phase != 'Running' && 
          resource.status.phase != 'Succeeded' &&
          has(resource.status.startTime) &&
          now - timestamp(resource.status.startTime) > duration('30m')
      nodeAssociation:
        expression: resource.spec.nodeName
      tracking:
        level: owner
      healthEvent:
        componentClass: Software
        isFatal: true
        message: "Network Operator pod has not reached Running state within 30 minutes"
        recommendedAction: CONTACT_SUPPORT
        errorCode:
          - NETWORK_OPERATOR_POD_UNHEALTHY
```

## Detection Logic

The policy triggers when **all** of the following conditions are true:

| Condition | Check |
| :-- | :-- |
| Namespace | Pod is in `gpu-operator` or `network-operator` namespace |
| Node assigned | Pod has `spec.nodeName` set (scheduled to a node) |
| Not healthy | Pod phase is NOT `Running` and NOT `Succeeded` |
| Time threshold | Pod has been in this state for more than 30 minutes |

### What This Catches

| Stuck State | Detected? |
| :-- | :-- |
| Stuck in init containers | Yes (after threshold) |
| `Pending` (scheduling/resource issues) | Yes (after threshold) |
| `CrashLoopBackOff` | Yes (after threshold) |
| `ImagePullBackOff` / `ErrImagePull` | Yes (after threshold) |
| `Failed` phase | Yes (after threshold) |
| Normal initialization (< threshold) | No (grace period) |
| `Running` | No (healthy) |
| `Succeeded` | No (completed successfully) |

## Owner-Level Tracking Behavior

When `tracking.level: owner` is configured, the kubernetes-object-monitor tracks pods by their owning DaemonSet rather than by individual pod name. This provides several benefits:

### Which Pods Are Tracked?

| Pod Owner | Tracked with `tracking.level: owner`? | Reason |
| :-- | :-- | :-- |
| DaemonSet | **Yes** | Replacements always appear on the same node |
| ReplicaSet/Deployment | **No** (silently skipped) | Replacements can be scheduled on any node |
| StatefulSet | **No** (silently skipped) | Replacements may go to different nodes |
| Job | **No** (silently skipped) | May not create replacement pods |
| No owner (standalone pod) | **No** (silently skipped) | No controller to create replacements |

> **Note:** When a pod is skipped due to having a non-DaemonSet owner, a debug log is emitted but no health event is published. This is intentional behavior.

### Scenario: Pod Deletion During Troubleshooting

1. **DaemonSet pod fails** → Node is cordoned
2. **Admin deletes the pod** (e.g., `kubectl delete pod`) → Node **stays cordoned**
3. **DaemonSet creates replacement pod** → If replacement is healthy, node is uncordoned
4. **If replacement also fails** → Node remains cordoned

### Scenario: DaemonSet Deletion

1. **DaemonSet pod fails** → Node is cordoned
2. **Admin deletes the DaemonSet** → Node is **immediately uncordoned** (owner no longer exists)

### Scenario: DaemonSet No Longer Targets Node

1. **DaemonSet pod fails** → Node is cordoned
2. **Admin updates DaemonSet nodeSelector** to exclude this node → Node is **uncordoned** (owner no longer targets this node)

### Tracking Levels

| Level | State Key Format | Use Case |
|-------|------------------|----------|
| `resource` (default) | `policyName/namespace/resourceName` | General resources, any pod type |
| `owner` | `policyName/namespace/ownerKind/ownerName/nodeName` | **DaemonSet pods only** - replacement tracking |

> **Warning:** If you use `tracking.level: owner` with a policy that matches non-DaemonSet pods (e.g., Deployment pods), those pods will be **silently ignored**. Use `tracking.level: resource` (or omit the tracking config) if you need to monitor non-DaemonSet pods.

## Configuration Options

### Adjusting the Time Threshold
You can adjust the `30m` (30 minutes) threshold based on your environment:
- `duration('10m')` - 10 minutes (more aggressive, may cause false positives for slow image pulls)
- `duration('1h')` - 1 hour (more lenient, delays detection of stuck pods)

Choose a value that exceeds your longest expected pod initialization time.

### How the Time Check Works
The `now - timestamp(resource.status.startTime) > duration('30m')` expression:
- `now` - Current timestamp (provided by the CEL environment)
- `timestamp(resource.status.startTime)` - When the pod was created
- `duration('30m')` - The threshold duration (30 minutes)

### Resync Period
The `resyncPeriod` controls how often the monitor re-evaluates all resources:
- Default: `5m` (5 minutes)
- For faster detection, reduce to `1m` or `30s`
- Trade-off: Lower values increase API server load

```yaml
kubernetes-object-monitor:
  resyncPeriod: 1m  # Re-evaluate every minute
```

## Troubleshooting

### Investigating Health Events
If you receive these events, investigate the pod status:

1.  **Check pod status and events:**
    ```bash
    kubectl get pods -n gpu-operator -o wide
    kubectl describe pod <pod-name> -n gpu-operator
    ```

2.  **Check container logs (if containers have started):**
    ```bash
    kubectl logs -n gpu-operator <pod-name>
    # For init containers:
    kubectl logs -n gpu-operator <pod-name> -c <init-container-name>
    ```

3.  **Common issues to look for:**
    - `ImagePullBackOff` - Check image name and registry credentials
    - `CrashLoopBackOff` - Check container logs for crash reason
    - `Pending` - Check node resources and scheduling constraints
    - Init container stuck - Check init container logs

4.  **Verify node health:**
    ```bash
    kubectl get node <node-name>
    kubectl describe node <node-name>
    ```

### Owner-Level Tracking Issues

#### Problem: Pods are not being monitored (no health events)

If you're using `tracking.level: owner` but not seeing health events for certain pods:

1. **Check if the pod has a DaemonSet owner:**
   ```bash
   kubectl get pod <pod-name> -n <namespace> -o jsonpath='{.metadata.ownerReferences[*].kind}'
   ```
   - If the output is `ReplicaSet`, `Job`, or empty, the pod will be **skipped** with owner-level tracking
   - Only pods with `DaemonSet` owner are tracked

2. **Check kubernetes-object-monitor logs for skip messages:**
   ```bash
   kubectl logs -n nvsentinel deployment/kubernetes-object-monitor | grep -i "skipping"
   ```
   You may see messages like:
   - `Skipping resource without controller owner for owner-level tracking`
   - `Skipping resource with non-DaemonSet owner for owner-level tracking`

3. **Solution:** If you need to monitor non-DaemonSet pods, either:
   - Remove the `tracking` section from your policy (defaults to resource-level tracking)
   - Explicitly set `tracking.level: resource`

#### Problem: Node stays cordoned after DaemonSet is healthy

1. **Check if the DaemonSet still targets the node:**
   ```bash
   kubectl get ds <daemonset-name> -n <namespace> -o yaml | grep -A 10 nodeSelector
   ```

2. **Check the policy match annotation on the node:**
   ```bash
   kubectl get node <node-name> -o jsonpath='{.metadata.annotations.nvsentinel\.dgxc\.nvidia\.com/policy-matches}'
   ```
