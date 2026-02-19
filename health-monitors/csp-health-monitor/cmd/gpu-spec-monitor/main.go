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

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/nvidia/nvsentinel/commons/pkg/logger"
	srv "github.com/nvidia/nvsentinel/commons/pkg/server"
	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/csp"
	awsgpu "github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/csp/aws"
	azuregpu "github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/csp/azure"
	gcpgpu "github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/csp/gcp"
	ocigpu "github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/csp/oci"
	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/model"
	"github.com/nvidia/nvsentinel/health-monitors/csp-health-monitor/pkg/nvml"
)

const (
	defaultMetricsPort         = "2114"
	defaultPollIntervalSeconds = "900"
	defaultUDSPath             = "/run/nvsentinel/nvsentinel.sock"

	agentName      = "gpu-spec-monitor"
	componentClass = "GPU"
	checkName      = "GPUCountMismatch"
	errorCode      = "GPU_COUNT_MISMATCH"

	udsMaxRetries = 5
	udsRetryDelay = 5 * time.Second

	cspStartupMaxRetries = 10
	cspStartupRetryDelay = 10 * time.Second
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"

	gpuExpectedCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gpu_expected_count",
			Help: "Expected GPU count for this node as reported by the CSP Cloud API.",
		},
		[]string{"csp", "instance_type", "region", "gpu_model"},
	)

	gpuSpecMonitorErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gpu_spec_monitor_errors_total",
			Help: "Total number of errors encountered during GPU spec validation.",
		},
		[]string{"csp"},
	)

	gpuActualCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gpu_actual_count",
		Help: "Actual GPU count visible to the node via NVML (only set when built with NVML).",
	})

	gpuCountMismatch = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gpu_count_mismatch",
		Help: "1 if expected GPU count (from CSP API) does not match actual count (from NVML), 0 otherwise.",
	})

	udsSendErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gpu_spec_monitor_uds_send_errors_total",
		Help: "Total errors encountered when sending health events via UDS.",
	})
)

type appConfig struct {
	cspName            string
	metricsPort        string
	pollInterval       string
	udsPath            string
	processingStrategy string
}

func parseFlags() *appConfig {
	cfg := &appConfig{}
	flag.StringVar(&cfg.cspName, "csp", "", "Cloud service provider name (aws | gcp | azure | oci).")
	flag.StringVar(&cfg.metricsPort, "metrics-port", defaultMetricsPort, "Port to expose Prometheus metrics on.")
	flag.StringVar(&cfg.pollInterval, "poll-interval", defaultPollIntervalSeconds, "Interval in seconds between GPU count discovery cycles.")
	flag.StringVar(&cfg.udsPath, "uds-path", defaultUDSPath, "Path to the Platform Connector UDS socket.")
	flag.StringVar(&cfg.processingStrategy, "processing-strategy", "EXECUTE_REMEDIATION",
		"Event processing strategy: EXECUTE_REMEDIATION or STORE_ONLY")
	flag.Parse()
	return cfg
}

func main() {
	logger.SetDefaultStructuredLogger("gpu-spec-monitor", version)
	slog.Info("Starting gpu-spec-monitor", "version", version, "commit", commit, "date", date)

	if err := run(); err != nil {
		slog.Error("gpu-spec-monitor exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := parseFlags()

	cspType, err := csp.ParseCSP(cfg.cspName)
	if err != nil {
		return err
	}

	provider := newProvider(cspType)
	if provider == nil {
		return nil
	}

	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		return fmt.Errorf("NODE_NAME env is required")
	}

	// Startup log for deployment verification across CSPs (AWS, GCP, Azure, OCI).
	slog.Info("gpu-spec-monitor config",
		"csp", cspType,
		"nodeName", nodeName,
		"udsPath", cfg.udsPath,
		"pollIntervalSeconds", cfg.pollInterval,
		"metricsPort", cfg.metricsPort,
		"processingStrategy", cfg.processingStrategy)

	strategy, ok := pb.ProcessingStrategy_value[cfg.processingStrategy]
	if !ok {
		return fmt.Errorf("invalid --processing-strategy %q", cfg.processingStrategy)
	}

	conn, udsClient, err := setupUDSConnection(cfg.udsPath)
	if err != nil {
		return err
	}
	defer conn.Close()

	pollInterval, err := time.ParseDuration(cfg.pollInterval + "s")
	if err != nil || pollInterval <= 0 {
		slog.Warn("Invalid --poll-interval, using default",
			"value", cfg.pollInterval,
			"default", defaultPollIntervalSeconds)
		pollInterval = 900 * time.Second
	}

	metricsPort, err := parsePort(cfg.metricsPort)
	if err != nil {
		return fmt.Errorf("invalid --metrics-port: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	expectedSpec, err := fetchExpectedGPUSpec(ctx, provider)
	if err != nil {
		return fmt.Errorf("failed to fetch expected GPU spec from CSP API: %w", err)
	}
	if expectedSpec.ExpectedGPUs == 0 {
		return fmt.Errorf("CSP API returned 0 expected GPUs for instance type %s (%s); cannot run GPU spec monitor",
			expectedSpec.InstanceType, expectedSpec.CSP)
	}

	gpuExpectedCount.WithLabelValues(
		string(expectedSpec.CSP),
		expectedSpec.InstanceType,
		expectedSpec.Region,
		expectedSpec.GPUModel,
	).Set(float64(expectedSpec.ExpectedGPUs))

	slog.Info("GPU count fetched from CSP (one-time)",
		"csp", expectedSpec.CSP,
		"instanceType", expectedSpec.InstanceType,
		"region", expectedSpec.Region,
		"expectedGPUs", expectedSpec.ExpectedGPUs,
		"gpuModel", expectedSpec.GPUModel)

	server := srv.NewServer(
		srv.WithPort(metricsPort),
		srv.WithPrometheusMetrics(),
		srv.WithSimpleHealth(),
	)

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		slog.Info("Starting metrics server", "port", metricsPort)
		if err := server.Serve(gCtx); err != nil {
			slog.Error("Metrics server failed - continuing without metrics", "error", err)
		}
		return nil
	})

	g.Go(func() error {
		slog.Info("Starting NVML GPU check loop",
			"pollInterval", pollInterval,
			"expectedGPUs", expectedSpec.ExpectedGPUs)

		var prevMismatch bool

		check := func() error {
			mismatch, err := checkActualGPUCount(gCtx, expectedSpec, udsClient, nodeName, pb.ProcessingStrategy(strategy), prevMismatch)
			if err != nil {
				return err
			}
			prevMismatch = mismatch
			return nil
		}

		if err := check(); err != nil {
			return err
		}

		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-gCtx.Done():
				slog.Info("Context cancelled, stopping NVML GPU check loop")
				return nil
			case <-ticker.C:
				if err := check(); err != nil {
					return err
				}
			}
		}
	})

	slog.Info("gpu-spec-monitor started successfully")

	if err := g.Wait(); err != nil {
		return err
	}

	slog.Info("gpu-spec-monitor shut down completed")
	return nil
}

func setupUDSConnection(udsPath string) (*grpc.ClientConn, pb.PlatformConnectorClient, error) {
	target := fmt.Sprintf("unix:%s", udsPath)

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to Platform Connector UDS at %s: %w", target, err)
	}

	slog.Info("Connected to Platform Connector UDS", "path", udsPath)
	return conn, pb.NewPlatformConnectorClient(conn), nil
}

func newProvider(cspType model.CSP) csp.GPUCountProvider {
	switch cspType {
	case model.CSPAWS:
		return awsgpu.NewGPUCountProvider()
	case model.CSPGCP:
		return gcpgpu.NewGPUCountProvider()
	case model.CSPAzure:
		return azuregpu.NewGPUCountProvider()
	case model.CSPOCI:
		return ocigpu.NewGPUCountProvider()
	default:
		return nil
	}
}

// fetchExpectedGPUSpec calls the CSP API once at startup with exponential
// backoff retries.  The expected GPU count doesn't change for the lifetime
// of a node, so a single call is sufficient.
func fetchExpectedGPUSpec(ctx context.Context, provider csp.GPUCountProvider) (*csp.GPUCountResult, error) {
	backoff := wait.Backoff{
		Steps:    cspStartupMaxRetries,
		Duration: cspStartupRetryDelay,
		Factor:   2.0,
		Jitter:   0.1,
		Cap:      5 * time.Minute,
	}

	var result *csp.GPUCountResult
	var lastErr error

	err := wait.ExponentialBackoffWithContext(ctx, backoff, func(ctx context.Context) (bool, error) {
		queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		r, attemptErr := provider.GetExpectedGPUCount(queryCtx)
		if attemptErr == nil {
			result = r
			return true, nil
		}

		lastErr = attemptErr
		gpuSpecMonitorErrors.WithLabelValues(string(provider.GetName())).Inc()
		slog.Warn("CSP API call failed, retrying",
			"csp", provider.GetName(),
			"error", attemptErr)
		return false, nil
	})

	if wait.Interrupted(err) {
		slog.Error("CSP API failed after retries",
			"csp", provider.GetName(),
			"retries", cspStartupMaxRetries,
			"lastError", lastErr)
		return nil, fmt.Errorf("exhausted %d retries: %w", cspStartupMaxRetries, lastErr)
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

// checkActualGPUCount queries NVML, compares against the cached CSP spec,
// and publishes health events on state transitions only.  Returns (mismatch, nil)
// or (false, error) if NVML is unavailable (fatal).
func checkActualGPUCount(
	ctx context.Context,
	spec *csp.GPUCountResult,
	udsClient pb.PlatformConnectorClient,
	nodeName string,
	processingStrategy pb.ProcessingStrategy,
	prevMismatch bool,
) (bool, error) {
	actualGPUs, actualAvailable := nvml.GetDeviceCount()
	if !actualAvailable {
		gpuActualCount.Set(0)
		gpuCountMismatch.Set(0)
		err := fmt.Errorf("NVML unavailable: cannot get GPU count (expected %d from CSP)", spec.ExpectedGPUs)
		slog.Error("NVML unavailable, GPU count check failed", "expectedGPUs", spec.ExpectedGPUs, "error", err)
		return false, err
	}

	slog.Info("GPU count fetched from NVML", "actualGPUs", actualGPUs)
	gpuActualCount.Set(float64(actualGPUs))

	currentMismatch := actualGPUs != spec.ExpectedGPUs
	slog.Info("Comparing GPU counts: CSP expected vs NVML actual",
		"expected", spec.ExpectedGPUs,
		"actual", actualGPUs,
		"match", !currentMismatch)

	if currentMismatch {
		gpuCountMismatch.Set(1.0)
		slog.Warn("GPU count mismatch detected",
			"expected", spec.ExpectedGPUs,
			"actual", actualGPUs,
			"csp", spec.CSP,
			"instanceType", spec.InstanceType)

		if !prevMismatch {
			event := buildMismatchEvent(spec, actualGPUs, nodeName, processingStrategy)
			if err := sendHealthEventWithRetry(ctx, udsClient, event); err != nil {
				slog.Error("Failed to send GPU mismatch health event", "error", err)
			}
		}
	} else {
		gpuCountMismatch.Set(0.0)

		if prevMismatch {
			event := buildHealthyEvent(spec, nodeName, processingStrategy)
			if err := sendHealthEventWithRetry(ctx, udsClient, event); err != nil {
				slog.Error("Failed to send GPU healthy event", "error", err)
			}
		}

		slog.Info("GPU count OK",
			"expected", spec.ExpectedGPUs,
			"actual", actualGPUs)
	}

	return currentMismatch, nil
}

func buildMismatchEvent(result *csp.GPUCountResult, actualGPUs int, nodeName string, strategy pb.ProcessingStrategy) *pb.HealthEvent {
	return &pb.HealthEvent{
		Version:            1,
		Agent:              agentName,
		ComponentClass:     componentClass,
		CheckName:          checkName,
		IsFatal:            true,
		IsHealthy:          false,
		NodeName:           nodeName,
		GeneratedTimestamp: timestamppb.Now(),
		RecommendedAction:  pb.RecommendedAction_CONTACT_SUPPORT,
		ErrorCode:          []string{errorCode},
		ProcessingStrategy: strategy,
		Message: fmt.Sprintf(
			"GPU count mismatch: CSP API reports %d GPUs for %s (%s) but node sees %d via NVML",
			result.ExpectedGPUs, result.InstanceType, result.CSP, actualGPUs,
		),
		Metadata: map[string]string{
			"csp":           string(result.CSP),
			"instance_type": result.InstanceType,
			"region":        result.Region,
			"gpu_model":     result.GPUModel,
			"expected_gpus": fmt.Sprintf("%d", result.ExpectedGPUs),
			"actual_gpus":   fmt.Sprintf("%d", actualGPUs),
		},
	}
}

func buildHealthyEvent(result *csp.GPUCountResult, nodeName string, strategy pb.ProcessingStrategy) *pb.HealthEvent {
	return &pb.HealthEvent{
		Version:            1,
		Agent:              agentName,
		ComponentClass:     componentClass,
		CheckName:          checkName,
		IsFatal:            false,
		IsHealthy:          true,
		NodeName:           nodeName,
		GeneratedTimestamp: timestamppb.Now(),
		RecommendedAction:  pb.RecommendedAction_NONE,
		ErrorCode:          []string{errorCode},
		ProcessingStrategy: strategy,
		Message: fmt.Sprintf(
			"GPU count recovered: CSP API reports %d GPUs and node now sees %d via NVML",
			result.ExpectedGPUs, result.ExpectedGPUs,
		),
		Metadata: map[string]string{
			"csp":           string(result.CSP),
			"instance_type": result.InstanceType,
			"region":        result.Region,
			"gpu_model":     result.GPUModel,
		},
	}
}

// sendHealthEventWithRetry sends a health event via UDS with exponential
// backoff, matching the retry pattern used by the maintenance-notifier
// trigger engine.
func sendHealthEventWithRetry(ctx context.Context, client pb.PlatformConnectorClient, event *pb.HealthEvent) error {
	backoff := wait.Backoff{
		Steps:    udsMaxRetries,
		Duration: udsRetryDelay,
		Factor:   1.5,
		Jitter:   0.1,
	}

	var lastErr error

	err := wait.ExponentialBackoffWithContext(ctx, backoff, func(ctx context.Context) (bool, error) {
		_, attemptErr := client.HealthEventOccurredV1(ctx, &pb.HealthEvents{
			Version: 1,
			Events:  []*pb.HealthEvent{event},
		})
		if attemptErr == nil {
			slog.Info("Published health event to platform-connector",
				"checkName", event.CheckName,
				"isHealthy", event.IsHealthy,
				"node", event.NodeName)
			return true, nil
		}

		lastErr = attemptErr
		udsSendErrors.Inc()

		if isRetryableGRPCError(attemptErr) {
			slog.Warn("Retryable error sending health event via UDS",
				"node", event.NodeName,
				"error", attemptErr)
			return false, nil
		}

		slog.Error("Non-retryable error sending health event via UDS",
			"node", event.NodeName,
			"error", attemptErr)
		return false, attemptErr
	})

	if wait.Interrupted(err) {
		return fmt.Errorf("failed to send health event after %d retries (timeout): %w", udsMaxRetries, lastErr)
	}
	return err
}

func isRetryableGRPCError(err error) bool {
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.Unavailable
	}
	return false
}

func parsePort(s string) (int, error) {
	port := 0
	_, err := fmt.Sscanf(s, "%d", &port)
	if err != nil || port <= 0 || port > 65535 {
		return 0, fmt.Errorf("invalid port %q", s)
	}
	return port, nil
}
