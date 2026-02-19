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

package oci

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/common/auth"
	"github.com/oracle/oci-go-sdk/v65/core"

	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/csp"
	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/model"
)

// GPUCountProvider queries OCI IMDS and Compute API to determine the expected
// GPU count for the current instance.
type GPUCountProvider struct{}

// NewGPUCountProvider returns a new OCI GPU count provider.
func NewGPUCountProvider() *GPUCountProvider {
	return &GPUCountProvider{}
}

// GetName returns the CSP identifier.
func (p *GPUCountProvider) GetName() model.CSP {
	return model.CSPOCI
}

// imdsMetadata holds the fields from OCI IMDS v2.
type imdsMetadata struct {
	ID                  string `json:"id"`
	Shape               string `json:"shape"`
	CanonicalRegionName string `json:"canonicalRegionName"`
	CompartmentID       string `json:"compartmentId"`
}

// GetExpectedGPUCount queries OCI IMDS for instance metadata, authenticates
// via OKE Workload Identity or credentials file, and calls GetInstance to
// extract GPU count from shapeConfig.
func (p *GPUCountProvider) GetExpectedGPUCount(ctx context.Context) (*csp.GPUCountResult, error) {
	body, err := csp.IMDSGet("http://169.254.169.254/opc/v2/instance/", map[string]string{
		"Authorization": "Bearer Oracle",
	})
	if err != nil {
		return nil, fmt.Errorf("getting OCI IMDS: %w", err)
	}

	var meta imdsMetadata
	if err := json.Unmarshal(body, &meta); err != nil {
		return nil, fmt.Errorf("parsing OCI IMDS: %w", err)
	}

	slog.Info("OCI IMDS metadata retrieved",
		"instanceOCID", meta.ID,
		"shape", meta.Shape,
		"region", meta.CanonicalRegionName)

	var cfgProvider common.ConfigurationProvider
	if os.Getenv("OCI_CREDENTIALS_FILE") != "" {
		cfgProvider = common.CustomProfileConfigProvider(
			os.Getenv("OCI_CREDENTIALS_FILE"),
			os.Getenv("OCI_PROFILE"),
		)
		slog.Info("OCI: using credentials file auth")
	} else {
		cfgProvider, err = auth.OkeWorkloadIdentityConfigurationProvider()
		if err != nil {
			return nil, fmt.Errorf("OKE workload identity auth failed: %w", err)
		}
		slog.Info("OCI: using OKE Workload Identity auth")
	}

	computeClient, err := core.NewComputeClientWithConfigurationProvider(cfgProvider)
	if err != nil {
		return nil, fmt.Errorf("creating OCI compute client: %w", err)
	}

	computeClient.SetRegion(meta.CanonicalRegionName)

	resp, err := computeClient.GetInstance(ctx, core.GetInstanceRequest{
		InstanceId: common.String(meta.ID),
	})
	if err != nil {
		return nil, fmt.Errorf("OCI GetInstance failed: %w", err)
	}

	instance := resp.Instance
	totalGPUs := 0
	gpuModel := ""

	if instance.ShapeConfig != nil {
		if instance.ShapeConfig.Gpus != nil {
			totalGPUs = int(*instance.ShapeConfig.Gpus)
		}
		if instance.ShapeConfig.GpuDescription != nil {
			gpuModel = *instance.ShapeConfig.GpuDescription
		}
	}
	if totalGPUs == 0 {
		return nil, fmt.Errorf("OCI instance shape %s has no GPUs in ShapeConfig", meta.Shape)
	}

	return &csp.GPUCountResult{
		CSP:          model.CSPOCI,
		InstanceType: meta.Shape,
		Region:       meta.CanonicalRegionName,
		ExpectedGPUs: totalGPUs,
		GPUModel:     gpuModel,
	}, nil
}
