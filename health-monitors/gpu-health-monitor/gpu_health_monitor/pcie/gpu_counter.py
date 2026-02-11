# Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import logging as log

NVIDIA_VENDOR_ID = "0x10de"
GPU_CLASS_PREFIX = "0x0302"  # GPU Class prefix for a100,h100 etc


def get_nvidia_gpu_pci_addresses(sysfs_base: str = "/sys") -> list[str]:
    """Scan sysfs for NVIDIA GPU PCI addresses.

    Reads /sys/bus/pci/devices/*/vendor and /sys/bus/pci/devices/*/class
    to find NVIDIA GPUs (vendor 0x10de, class prefix 0x0302).

    Args:
        sysfs_base: Base path for sysfs. Default "/sys". Override for testing.

    Returns:
        List of PCI addresses like ['0000:04:00.0', '0000:05:00.0', ...]
    """
    devices_path = os.path.join(sysfs_base, "bus", "pci", "devices")
    gpu_addresses = []

    try:
        entries = os.listdir(devices_path)
    except OSError as e:
        log.exception(f"Failed to read PCI devices directory {devices_path}")
        return []

    for entry in sorted(entries):
        device_path = os.path.join(devices_path, entry)

        vendor = _read_sysfs_file(os.path.join(device_path, "vendor"))
        if vendor != NVIDIA_VENDOR_ID:
            continue

        device_class = _read_sysfs_file(os.path.join(device_path, "class"))
        if not device_class or not device_class.startswith(GPU_CLASS_PREFIX):
            continue

        gpu_addresses.append(entry)  # entry is the PCI address like "0000:04:00.0"
        log.debug(f"Found NVIDIA GPU on PCIe bus: {entry} (class={device_class})")

    log.info(f"Sysfs GPU enumeration complete: {len(gpu_addresses)} NVIDIA GPUs found")
    return gpu_addresses


def _read_sysfs_file(path: str) -> str:
    """Read and strip a sysfs file. Returns empty string on error."""
    try:
        with open(path, "r") as f:
            return f.read().strip()
    except OSError:
        return ""
