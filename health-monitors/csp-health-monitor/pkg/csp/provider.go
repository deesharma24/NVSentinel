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

package csp

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/model"
)

// GPUCountProvider is the interface that each CSP must implement to report
// the expected GPU count for the node it runs on.
type GPUCountProvider interface {
	GetExpectedGPUCount(ctx context.Context) (*GPUCountResult, error)
	GetName() model.CSP
}

// GPUCountResult holds the expected GPU count and metadata from the CSP API.
type GPUCountResult struct {
	CSP          model.CSP
	InstanceType string
	Region       string
	ExpectedGPUs int
	GPUModel     string
}

// ParseCSP validates a CSP name string and returns the corresponding model.CSP.
// Actual provider construction is done by the caller using the returned CSP
// and a switch in cmd/gpu-spec-monitor/main.go to avoid pulling all SDKs
// into every binary.
func ParseCSP(name string) (model.CSP, error) {
	c := model.CSP(strings.ToLower(strings.TrimSpace(name)))
	switch c {
	case model.CSPAWS, model.CSPGCP, model.CSPAzure, model.CSPOCI:
		return c, nil
	case "":
		return "", fmt.Errorf("CSP name is required (aws | gcp | azure | oci)")
	default:
		return "", fmt.Errorf("unsupported CSP: %q (expected: aws | gcp | azure | oci)", c)
	}
}

// IMDSGet performs an HTTP GET against an IMDS endpoint with optional headers.
// Used by Azure and OCI providers that lack a native IMDS SDK client.
func IMDSGet(url string, headers map[string]string) ([]byte, error) {
	client := &http.Client{Timeout: 5 * time.Second}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating IMDS request for %s: %w", url, err)
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("querying IMDS %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("IMDS %s returned status %d", url, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading IMDS response from %s: %w", url, err)
	}

	return body, nil
}
