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
import tempfile
import pytest
from gpu_health_monitor.pcie.gpu_counter import get_nvidia_gpu_pci_addresses


def _create_fake_device(base: str, addr: str, vendor: str, device_class: str) -> None:
    """Create a fake sysfs PCI device entry for testing."""
    device_dir = os.path.join(base, "bus", "pci", "devices", addr)
    os.makedirs(device_dir, exist_ok=True)
    with open(os.path.join(device_dir, "vendor"), "w") as f:
        f.write(vendor)
    with open(os.path.join(device_dir, "class"), "w") as f:
        f.write(device_class)


class TestGetNvidiaGpuPciAddresses:
    def test_finds_nvidia_gpus(self):
        """Test that NVIDIA GPUs with correct class are detected."""
        with tempfile.TemporaryDirectory() as base:
            _create_fake_device(base, "0000:04:00.0", "0x10de", "0x030200")
            _create_fake_device(base, "0000:05:00.0", "0x10de", "0x030200")
            result = get_nvidia_gpu_pci_addresses(sysfs_base=base)
            assert result == ["0000:04:00.0", "0000:05:00.0"]

    def test_filters_nvidia_nvswitch(self):
        """Test that NVIDIA NVSwitch devices (class 0x0680) are filtered out."""
        with tempfile.TemporaryDirectory() as base:
            _create_fake_device(base, "0000:04:00.0", "0x10de", "0x030200")
            _create_fake_device(base, "0000:c5:00.0", "0x10de", "0x068000")  # NVSwitch
            result = get_nvidia_gpu_pci_addresses(sysfs_base=base)
            assert result == ["0000:04:00.0"]

    def test_empty_directory(self):
        """Test that an empty sysfs devices directory returns empty list."""
        with tempfile.TemporaryDirectory() as base:
            devices_path = os.path.join(base, "bus", "pci", "devices")
            os.makedirs(devices_path, exist_ok=True)
            result = get_nvidia_gpu_pci_addresses(sysfs_base=base)
            assert result == []

    def test_nonexistent_directory(self):
        """Test that a non-existent sysfs path returns empty list without crashing."""
        result = get_nvidia_gpu_pci_addresses(sysfs_base="/nonexistent/path")
        assert result == []

    def test_multiple_gpus_sorted(self):
        """Test that multiple GPUs are returned in sorted order."""
        with tempfile.TemporaryDirectory() as base:
            # Create in non-sorted order
            _create_fake_device(base, "0000:82:00.0", "0x10de", "0x030200")
            _create_fake_device(base, "0000:04:00.0", "0x10de", "0x030200")
            _create_fake_device(base, "0000:41:00.0", "0x10de", "0x030200")
            _create_fake_device(base, "0000:c1:00.0", "0x10de", "0x030200")
            result = get_nvidia_gpu_pci_addresses(sysfs_base=base)
            assert result == ["0000:04:00.0", "0000:41:00.0", "0000:82:00.0", "0000:c1:00.0"]

    def test_missing_vendor_file(self):
        """Test that devices with missing vendor file are skipped."""
        with tempfile.TemporaryDirectory() as base:
            _create_fake_device(base, "0000:04:00.0", "0x10de", "0x030200")
            # Create device with only class file (no vendor)
            device_dir = os.path.join(base, "bus", "pci", "devices", "0000:05:00.0")
            os.makedirs(device_dir, exist_ok=True)
            with open(os.path.join(device_dir, "class"), "w") as f:
                f.write("0x030200")
            result = get_nvidia_gpu_pci_addresses(sysfs_base=base)
            assert result == ["0000:04:00.0"]

    def test_missing_class_file(self):
        """Test that NVIDIA devices with missing class file are skipped."""
        with tempfile.TemporaryDirectory() as base:
            _create_fake_device(base, "0000:04:00.0", "0x10de", "0x030200")
            # Create device with only vendor file (no class)
            device_dir = os.path.join(base, "bus", "pci", "devices", "0000:05:00.0")
            os.makedirs(device_dir, exist_ok=True)
            with open(os.path.join(device_dir, "vendor"), "w") as f:
                f.write("0x10de")
            result = get_nvidia_gpu_pci_addresses(sysfs_base=base)
            assert result == ["0000:04:00.0"]

    def test_eight_h100_gpus(self):
        """Test realistic 8x H100 GPU scenario."""
        with tempfile.TemporaryDirectory() as base:
            gpu_addresses = [
                "0000:04:00.0",
                "0000:05:00.0",
                "0000:41:00.0",
                "0000:42:00.0",
                "0000:82:00.0",
                "0000:83:00.0",
                "0000:c1:00.0",
                "0000:c2:00.0",
            ]
            # Also add NVSwitches and other NVIDIA devices
            for addr in gpu_addresses:
                _create_fake_device(base, addr, "0x10de", "0x030200")
            _create_fake_device(base, "0000:c5:00.0", "0x10de", "0x068000")  # NVSwitch
            _create_fake_device(base, "0000:c6:00.0", "0x10de", "0x068000")  # NVSwitch
            _create_fake_device(base, "0000:00:00.0", "0x8086", "0x060000")  # Intel host bridge

            result = get_nvidia_gpu_pci_addresses(sysfs_base=base)
            assert result == gpu_addresses
            assert len(result) == 8
