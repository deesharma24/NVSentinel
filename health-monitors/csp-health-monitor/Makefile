# csp-health-monitor Makefile

# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.

# =============================================================================
# MODULE-SPECIFIC CONFIGURATION
# =============================================================================

IS_GO_MODULE := 1
IS_KO_MODULE := 1
HAS_DOCKER := 1

# =============================================================================
# INCLUDE SHARED DEFINITIONS
# =============================================================================

include ../../make/common.mk
include ../../make/go.mk
include ../../make/docker.mk
# Override docker-build: this module only has Dockerfile.gpu-spec-monitor (no generic Dockerfile)
docker-build: docker-build-gpu-spec-monitor
	@echo "csp-health-monitor docker-build delegates to gpu-spec-monitor image."

# Test setup commands for kubebuilder envtest
# Version is centrally managed in .versions.yaml
TEST_SETUP_COMMANDS := \
	go install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(SETUP_ENVTEST_VERSION) && \
	eval $$(setup-envtest use --use-env -p env) &&

# =============================================================================
# DEFAULT TARGET
# =============================================================================

.PHONY: all
all: lint-test

# =============================================================================
# MODULE HELP
# =============================================================================

# -----------------------------------------------------------------------------
# gpu-spec-monitor image (CUDA base + NVML for actual GPU count)
# Built via Dockerfile.gpu-spec-monitor, not ko.
# -----------------------------------------------------------------------------
GPU_SPEC_MONITOR_IMAGE := $(CONTAINER_REGISTRY)/$(CONTAINER_ORG)/nvsentinel/gpu-spec-monitor

.PHONY: docker-build-gpu-spec-monitor
docker-build-gpu-spec-monitor: setup-buildx
	@echo "Building gpu-spec-monitor (CUDA base, NVML)..."
	cd $(REPO_ROOT) && docker buildx build \
		--platform $(PLATFORMS) \
		--network=host \
		$(CACHE_FROM_ARG) \
		$(CACHE_TO_ARG) \
		--build-arg VERSION=$(VERSION) \
		--build-arg GIT_COMMIT=$(GIT_COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		$(DOCKER_LOAD_ARG) \
		-t $(GPU_SPEC_MONITOR_IMAGE):$(SAFE_REF_NAME) \
		-f $(MODULE_PATH)/Dockerfile.gpu-spec-monitor \
		.

.PHONY: docker-publish-gpu-spec-monitor
docker-publish-gpu-spec-monitor: setup-buildx
	@echo "Building and publishing gpu-spec-monitor..."
	cd $(REPO_ROOT) && docker buildx build \
		--platform $(PLATFORMS) \
		--network=host \
		$(CACHE_FROM_ARG) \
		$(CACHE_TO_ARG) \
		--build-arg VERSION=$(VERSION) \
		--build-arg GIT_COMMIT=$(GIT_COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		--push \
		-t $(GPU_SPEC_MONITOR_IMAGE):$(SAFE_REF_NAME) \
		-f $(MODULE_PATH)/Dockerfile.gpu-spec-monitor \
		.

.PHONY: help
help:
	@echo "csp-health-monitor Makefile - Using nvsentinel make/*.mk standards"
	@echo ""
	@echo "Main targets: all, lint-test, ci-test, build, test, lint, clean"
	@echo "Ko targets: ko-build, ko-publish"
	@echo "Docker (gpu-spec-monitor, CUDA+NVML): docker-build-gpu-spec-monitor, docker-publish-gpu-spec-monitor"
	@echo ""
	@echo "Build notes:"
	@echo "  - csp-health-monitor and maintenance-notifier: built using ko"
	@echo "  - gpu-spec-monitor: built via Dockerfile.gpu-spec-monitor (CUDA image, NVML for actual GPU count)"
	@echo "  - Use 'make ko-build' for local builds (KO_DOCKER_REPO=ko.local by default)"
	@echo "  - Use 'make docker-build-gpu-spec-monitor' to build the gpu-spec-monitor image locally"
	@echo "  - For Tilt development, see Tiltfile"

