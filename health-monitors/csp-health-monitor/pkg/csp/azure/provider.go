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

package azure

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v5"

	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/csp"
	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/model"
)

// GPUCountProvider queries Azure IMDS and Resource SKUs to determine the
// expected GPU count for the current instance.
type GPUCountProvider struct{}

// NewGPUCountProvider returns a new Azure GPU count provider.
func NewGPUCountProvider() *GPUCountProvider {
	return &GPUCountProvider{}
}

// GetName returns the CSP identifier.
func (p *GPUCountProvider) GetName() model.CSP {
	return model.CSPAzure
}

// imdsResponse holds the fields from the Azure IMDS endpoint.
type imdsResponse struct {
	Compute struct {
		VMSize         string `json:"vmSize"`
		SubscriptionID string `json:"subscriptionId"`
		Location       string `json:"location"`
		TagsList       []struct {
			Name  string `json:"name"`
			Value string `json:"value"`
		} `json:"tagsList"`
	} `json:"compute"`
}

// GetExpectedGPUCount queries Azure IMDS for VM metadata, authenticates with
// managed identity, and looks up the SKU to extract GPU count.
func (p *GPUCountProvider) GetExpectedGPUCount(ctx context.Context) (*csp.GPUCountResult, error) {
	imdsBytes, err := csp.IMDSGet(
		"http://169.254.169.254/metadata/instance?api-version=2021-02-01",
		map[string]string{"Metadata": "true"},
	)
	if err != nil {
		return nil, fmt.Errorf("getting Azure IMDS: %w", err)
	}

	var imdsResp imdsResponse
	if err := json.Unmarshal(imdsBytes, &imdsResp); err != nil {
		return nil, fmt.Errorf("parsing Azure IMDS response: %w", err)
	}

	vmSize := imdsResp.Compute.VMSize
	subscriptionID := imdsResp.Compute.SubscriptionID
	location := imdsResp.Compute.Location

	clientID := ""
	for _, tag := range imdsResp.Compute.TagsList {
		if tag.Name == "aks-managed-kubeletIdentityClientID" {
			clientID = tag.Value
			break
		}
	}

	slog.Info("Azure IMDS metadata retrieved",
		"vmSize", vmSize,
		"subscriptionId", subscriptionID,
		"location", location,
		"clientId", clientID)

	var cred *azidentity.ManagedIdentityCredential
	if clientID != "" {
		cred, err = azidentity.NewManagedIdentityCredential(&azidentity.ManagedIdentityCredentialOptions{
			ID: azidentity.ClientID(clientID),
		})
	} else {
		cred, err = azidentity.NewManagedIdentityCredential(nil)
	}

	if err != nil {
		return nil, fmt.Errorf("creating Azure credential: %w", err)
	}

	skuClient, err := armcompute.NewResourceSKUsClient(subscriptionID, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("creating Azure SKU client: %w", err)
	}

	totalGPUs := 0
	gpuModel := ""
	found := false

	pager := skuClient.NewListPager(&armcompute.ResourceSKUsClientListOptions{
		Filter: strPtr(fmt.Sprintf("location eq '%s'", location)),
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing Azure SKUs: %w", err)
		}

		for _, sku := range page.Value {
			if sku.Name == nil || !strings.EqualFold(*sku.Name, vmSize) {
				continue
			}
			if sku.ResourceType == nil || !strings.EqualFold(*sku.ResourceType, "virtualMachines") {
				continue
			}

			found = true

			for _, cap := range sku.Capabilities {
				if cap.Name != nil && *cap.Name == "GPUs" && cap.Value != nil {
					gpuCount, err := strconv.Atoi(*cap.Value)
					if err == nil {
						totalGPUs = gpuCount
					}
				}
			}

			gpuModel = inferGPUModel(vmSize)

			break
		}

		if found {
			break
		}
	}

	if !found {
		return nil, fmt.Errorf("Azure SKU not found for vmSize=%s in location=%s", vmSize, location)
	}
	if totalGPUs == 0 {
		return nil, fmt.Errorf("Azure vmSize %s has no GPUs in SKU capabilities", vmSize)
	}

	return &csp.GPUCountResult{
		CSP:          model.CSPAzure,
		InstanceType: vmSize,
		Region:       location,
		ExpectedGPUs: totalGPUs,
		GPUModel:     gpuModel,
	}, nil
}

func strPtr(s string) *string {
	return &s
}

// inferGPUModel extracts a GPU model hint from the Azure VM size name.
func inferGPUModel(vmSize string) string {
	lower := strings.ToLower(vmSize)
	switch {
	case strings.Contains(lower, "h100"):
		return "H100"
	case strings.Contains(lower, "a100"):
		return "A100"
	case strings.Contains(lower, "a10"):
		return "A10"
	case strings.Contains(lower, "v100"):
		return "V100"
	case strings.Contains(lower, "t4"):
		return "T4"
	default:
		return ""
	}
}
