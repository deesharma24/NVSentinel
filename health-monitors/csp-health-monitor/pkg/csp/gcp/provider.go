// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package gcp

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	compute "cloud.google.com/go/compute/apiv1"
	computepb "cloud.google.com/go/compute/apiv1/computepb"
	"cloud.google.com/go/compute/metadata"

	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/csp"
	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/model"
)

// GPUCountProvider queries GCP metadata and Compute API to determine the
// expected GPU count for the current instance.
type GPUCountProvider struct{}

// NewGPUCountProvider returns a new GCP GPU count provider.
func NewGPUCountProvider() *GPUCountProvider {
	return &GPUCountProvider{}
}

// GetName returns the CSP identifier.
func (p *GPUCountProvider) GetName() model.CSP {
	return model.CSPGCP
}

// GetExpectedGPUCount queries GCP metadata for instance info, then calls
// instances.get to extract GPU information from guestAccelerators.
func (p *GPUCountProvider) GetExpectedGPUCount(ctx context.Context) (*csp.GPUCountResult, error) {
	mc := metadata.NewClient(nil)

	project, err := mc.ProjectID()
	if err != nil {
		return nil, fmt.Errorf("getting GCP project: %w", err)
	}

	zoneFull, err := mc.Zone()
	if err != nil {
		return nil, fmt.Errorf("getting GCP zone: %w", err)
	}

	parts := strings.Split(zoneFull, "/")
	zone := parts[len(parts)-1]

	instanceName, err := mc.InstanceName()
	if err != nil {
		return nil, fmt.Errorf("getting GCP instance name: %w", err)
	}

	mtFull, err := mc.Get("instance/machine-type")
	if err != nil {
		return nil, fmt.Errorf("getting GCP machine type: %w", err)
	}

	mtParts := strings.Split(strings.TrimSpace(mtFull), "/")
	machineType := mtParts[len(mtParts)-1]

	slog.Info("GCP metadata retrieved",
		"project", project,
		"zone", zone,
		"instance", instanceName,
		"machineType", machineType)

	client, err := compute.NewInstancesRESTClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating GCP compute client: %w", err)
	}
	defer client.Close()

	inst, err := client.Get(ctx, &computepb.GetInstanceRequest{
		Project:  project,
		Zone:     zone,
		Instance: instanceName,
	})
	if err != nil {
		return nil, fmt.Errorf("GCP instances.get failed: %w", err)
	}

	totalGPUs := 0
	gpuModel := ""

	for _, acc := range inst.GetGuestAccelerators() {
		if acc.AcceleratorCount != nil {
			totalGPUs += int(*acc.AcceleratorCount)
		}
		if acc.AcceleratorType != nil && gpuModel == "" {
			accParts := strings.Split(*acc.AcceleratorType, "/")
			gpuModel = accParts[len(accParts)-1]
		}
	}

	if totalGPUs == 0 {
		return nil, fmt.Errorf("GCP: no GPUs found in guestAccelerators for instance %s (machineType=%s)", instanceName, machineType)
	}

	region := zone
	if idx := strings.LastIndex(zone, "-"); idx > 0 {
		region = zone[:idx]
	}

	return &csp.GPUCountResult{
		CSP:          model.CSPGCP,
		InstanceType: machineType,
		Region:       region,
		ExpectedGPUs: totalGPUs,
		GPUModel:     gpuModel,
	}, nil
}
