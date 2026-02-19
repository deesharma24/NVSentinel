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

package aws

import (
	"context"
	"fmt"
	"log/slog"

	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"

	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/csp"
	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/model"
)

// GPUCountProvider queries AWS IMDS and EC2 DescribeInstanceTypes to determine
// the expected GPU count for the current instance.
type GPUCountProvider struct{}

// NewGPUCountProvider returns a new AWS GPU count provider.
func NewGPUCountProvider() *GPUCountProvider {
	return &GPUCountProvider{}
}

// GetName returns the CSP identifier.
func (p *GPUCountProvider) GetName() model.CSP {
	return model.CSPAWS
}

// GetExpectedGPUCount queries AWS IMDS for instance metadata, then calls
// DescribeInstanceTypes to extract GPU information.
func (p *GPUCountProvider) GetExpectedGPUCount(ctx context.Context) (*csp.GPUCountResult, error) {
	cfg, err := awsConfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	imdsClient := imds.NewFromConfig(cfg)

	iidOutput, err := imdsClient.GetInstanceIdentityDocument(ctx, &imds.GetInstanceIdentityDocumentInput{})
	if err != nil {
		return nil, fmt.Errorf("getting AWS instance identity document: %w", err)
	}

	instanceType := iidOutput.InstanceType
	region := iidOutput.Region

	slog.Info("AWS IMDS metadata retrieved",
		"instanceType", instanceType,
		"region", region,
		"az", iidOutput.AvailabilityZone)

	cfgWithRegion, err := awsConfig.LoadDefaultConfig(ctx, awsConfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("loading AWS config with region: %w", err)
	}

	ec2Client := ec2.NewFromConfig(cfgWithRegion)

	resp, err := ec2Client.DescribeInstanceTypes(ctx, &ec2.DescribeInstanceTypesInput{
		InstanceTypes: []types.InstanceType{types.InstanceType(instanceType)},
	})
	if err != nil {
		return nil, fmt.Errorf("AWS DescribeInstanceTypes failed: %w", err)
	}

	if len(resp.InstanceTypes) == 0 {
		return nil, fmt.Errorf("AWS DescribeInstanceTypes returned no results for %s", instanceType)
	}

	totalGPUs := 0
	gpuModel := ""
	it := resp.InstanceTypes[0]

	if it.GpuInfo != nil {
		for _, gpu := range it.GpuInfo.Gpus {
			if gpu.Count != nil {
				totalGPUs += int(*gpu.Count)
			}
			if gpu.Name != nil && gpuModel == "" {
				gpuModel = *gpu.Name
			}
		}
	}
	if totalGPUs == 0 {
		return nil, fmt.Errorf("AWS instance type %s has no GPUs in GpuInfo", instanceType)
	}

	return &csp.GPUCountResult{
		CSP:          model.CSPAWS,
		InstanceType: instanceType,
		Region:       region,
		ExpectedGPUs: totalGPUs,
		GPUModel:     gpuModel,
	}, nil
}
