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

ARG DCGM_VERSION="4.2.0-1-ubuntu22.04"
ARG VERSION="dev"
ARG GIT_COMMIT="none"
ARG BUILD_DATE=$(date -u +%FT%TZ)

FROM public.ecr.aws/docker/library/python:3.10-bookworm AS build

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install poetry==2.2.1

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /app

# Build commons-python wheel (only production deps needed)
COPY commons-python/logger/ /commons-python/logger/
RUN --mount=type=cache,target=/tmp/poetry_cache \
    cd /commons-python/logger && poetry build --format wheel

# Build gpu-health-monitor wheel
COPY health-monitors/gpu-health-monitor/ .
RUN --mount=type=cache,target=/tmp/poetry_cache \
    poetry remove commons-python && \
    poetry build --format wheel


FROM nvcr.io/nvidia/cloud-native/dcgm:${DCGM_VERSION} AS runtime

RUN echo "Acquire::https::Verify-Peer \"false\";" > /etc/apt/apt.conf.d/99disable-cert-check

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    echo 'Acquire::Retries "3";' > /etc/apt/apt.conf.d/80-retries && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        apt-transport-https \
        ca-certificates \
        gnupg \
        python3-pip && \
    rm -rf /var/lib/apt/lists/*

ARG PYTHONPATH="/usr/share/datacenter-gpu-manager-4/bindings/python3"
ARG VERSION
ARG GIT_COMMIT
ARG BUILD_DATE

ENV PYTHONPATH=${PYTHONPATH} \
    PYTHONUNBUFFERED=1 \
    VERSION=${VERSION} \
    GIT_COMMIT=${GIT_COMMIT} \
    BUILD_DATE=${BUILD_DATE}

# Copy and install both wheels
COPY --from=build /commons-python/logger/dist/*.whl ./
COPY --from=build /app/dist/*.whl ./
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install ./commons_python*.whl ./gpu_health_monitor*.whl
