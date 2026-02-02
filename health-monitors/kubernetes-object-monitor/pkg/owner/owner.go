// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package owner provides utilities for working with Kubernetes owner references
// and determining if DaemonSets target specific nodes.
//
// This package is used for owner-level tracking in kubernetes-object-monitor,
// which allows pods to be tracked by their owning DaemonSet rather than by
// individual pod name. This ensures that when a DaemonSet pod is deleted,
// the node stays cordoned until a healthy replacement pod is running.
package owner

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	schedulingcorev1 "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// cacheTTL is how long cached DaemonSet and Node entries are valid.
	//
	// Trade-off considerations:
	// - Longer TTL = fewer API calls, better for large clusters (2000+ nodes)
	// - Shorter TTL = faster response to DaemonSet deletion/modification
	//
	// 15 minutes is chosen because:
	// 1. DaemonSet configurations rarely change in production
	// 2. Pod startup can take 10-15 minutes (image pull, init containers)
	// 3. Worst case: if DaemonSet is deleted, node uncordons within 15 min
	//
	// Note: This cache only affects the "should node stay cordoned after pod deletion"
	// check. Pod health monitoring uses direct watches and is not affected by this cache.
	cacheTTL = 30 * time.Second
)

// Info contains information about a resource's owner
type Info struct {
	Kind      string
	Name      string
	Namespace string
}

// cacheEntry holds a cached object with expiration
type cacheEntry struct {
	obj       any
	expiresAt time.Time
	notFound  bool
}

// Resolver provides methods for resolving owner references and checking node targeting.
// It includes a simple TTL cache to reduce API server load in large clusters.
type Resolver struct {
	client client.Client

	cacheMu sync.RWMutex
	cache   map[string]cacheEntry
}

// NewResolver creates a new owner resolver
func NewResolver(c client.Client) *Resolver {
	return &Resolver{
		client: c,
		cache:  make(map[string]cacheEntry),
	}
}

// GetControllerOwner returns the controller owner reference for an unstructured object.
// Returns nil if no controller owner is found.
func (r *Resolver) GetControllerOwner(obj *unstructured.Unstructured) *Info {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Controller != nil && *ref.Controller {
			return &Info{
				Kind:      ref.Kind,
				Name:      ref.Name,
				Namespace: obj.GetNamespace(),
			}
		}
	}

	return nil
}

// OwnerTargetsNode checks if the owner DaemonSet should have a pod running on
// the specified node. This is used to determine if a node should stay cordoned
// after a pod deletion.
//
// Currently only supports DaemonSet owners. For other owner types (ReplicaSet,
// StatefulSet, Job, etc.), this returns false, meaning the node will be uncordoned
// when the pod is deleted.
//
// Returns:
//   - targetsNode: true if the owner is a DaemonSet that exists and targets this node
//   - nodeExists: true if the node exists in the cluster
//   - error: if there was a problem checking (other than not found)
func (r *Resolver) OwnerTargetsNode(
	ctx context.Context,
	ownerInfo *Info,
	nodeName string,
) (targetsNode bool, nodeExists bool, err error) {
	if ownerInfo == nil {
		return false, true, nil // Assume node exists if we can't check
	}

	// Only DaemonSets are supported for owner-level tracking
	if ownerInfo.Kind != "DaemonSet" {
		slog.Debug("Owner-level tracking only supports DaemonSets, skipping",
			"kind", ownerInfo.Kind, "name", ownerInfo.Name)

		return false, true, nil // Assume node exists if we can't check
	}

	return r.daemonSetTargetsNode(ctx, ownerInfo.Namespace, ownerInfo.Name, nodeName)
}

// daemonSetTargetsNode checks if a DaemonSet should have a pod on the specified node
// Returns (targetsNode, nodeExists, error)
func (r *Resolver) daemonSetTargetsNode(
	ctx context.Context,
	namespace, name, nodeName string,
) (bool, bool, error) {
	ds, found, err := r.getCachedDaemonSet(ctx, namespace, name)
	if err != nil {
		return false, true, err
	}

	if !found {
		slog.Debug("DaemonSet not found, owner deleted", "namespace", namespace, "name", name)

		return false, true, nil // DaemonSet deleted, assume node exists
	}

	node, found, err := r.getCachedNode(ctx, nodeName)
	if err != nil {
		return false, true, err
	}

	if !found {
		slog.Debug("Node not found, node deleted", "node", nodeName)

		return false, false, nil // Node deleted
	}

	return nodeMatchesDaemonSet(ds, node), true, nil
}

// getCachedDaemonSet retrieves a DaemonSet from cache or fetches it from the API server
func (r *Resolver) getCachedDaemonSet(ctx context.Context, namespace, name string) (*appsv1.DaemonSet, bool, error) {
	cacheKey := fmt.Sprintf("ds/%s/%s", namespace, name)

	// Check cache first
	r.cacheMu.RLock()

	if entry, ok := r.cache[cacheKey]; ok && time.Now().Before(entry.expiresAt) {
		r.cacheMu.RUnlock()

		if entry.notFound {
			return nil, false, nil
		}

		return entry.obj.(*appsv1.DaemonSet), true, nil
	}

	r.cacheMu.RUnlock()

	// Fetch from API server
	ds := &appsv1.DaemonSet{}
	err := r.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, ds)

	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()

	if err != nil {
		if apierrors.IsNotFound(err) {
			r.cache[cacheKey] = cacheEntry{notFound: true, expiresAt: time.Now().Add(cacheTTL)}

			return nil, false, nil
		}

		return nil, false, fmt.Errorf("failed to get DaemonSet: %w", err)
	}

	r.cache[cacheKey] = cacheEntry{obj: ds, expiresAt: time.Now().Add(cacheTTL)}

	return ds, true, nil
}

// getCachedNode retrieves a Node from cache or fetches it from the API server
func (r *Resolver) getCachedNode(ctx context.Context, name string) (*v1.Node, bool, error) {
	cacheKey := fmt.Sprintf("node/%s", name)

	// Check cache first
	r.cacheMu.RLock()

	if entry, ok := r.cache[cacheKey]; ok && time.Now().Before(entry.expiresAt) {
		r.cacheMu.RUnlock()

		if entry.notFound {
			return nil, false, nil
		}

		return entry.obj.(*v1.Node), true, nil
	}

	r.cacheMu.RUnlock()

	// Fetch from API server
	node := &v1.Node{}
	err := r.client.Get(ctx, types.NamespacedName{Name: name}, node)

	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()

	if err != nil {
		if apierrors.IsNotFound(err) {
			r.cache[cacheKey] = cacheEntry{notFound: true, expiresAt: time.Now().Add(cacheTTL)}

			return nil, false, nil
		}

		return nil, false, fmt.Errorf("failed to get node: %w", err)
	}

	r.cache[cacheKey] = cacheEntry{obj: node, expiresAt: time.Now().Add(cacheTTL)}

	return node, true, nil
}

// nodeMatchesDaemonSet checks if a node matches the DaemonSet's scheduling requirements
// using Kubernetes component-helpers for accurate scheduling semantics.
func nodeMatchesDaemonSet(ds *appsv1.DaemonSet, node *v1.Node) bool {
	podSpec := &ds.Spec.Template.Spec

	// Check nodeSelector
	if podSpec.NodeSelector != nil {
		selector := labels.SelectorFromSet(podSpec.NodeSelector)

		if !selector.Matches(labels.Set(node.Labels)) {
			slog.Debug("Node doesn't match DaemonSet nodeSelector",
				"daemonset", ds.Name, "node", node.Name)

			return false
		}
	}

	// Check node affinity using K8s component-helpers
	if podSpec.Affinity != nil &&
		podSpec.Affinity.NodeAffinity != nil &&
		podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		nodeSelector := podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution

		match, err := schedulingcorev1.MatchNodeSelectorTerms(node, nodeSelector)
		if err != nil {
			slog.Debug("Error matching node selector terms",
				"daemonset", ds.Name, "node", node.Name, "error", err)

			return false
		}

		if !match {
			slog.Debug("Node doesn't match DaemonSet node affinity",
				"daemonset", ds.Name, "node", node.Name)

			return false
		}
	}

	// Check tolerations using K8s component-helpers
	// FindMatchingUntoleratedTaint returns the first taint that is not tolerated
	// The last parameter (enableComparisonOperators) is false as we don't need Gt/Lt operators for taints
	_, hasUntoleratedTaint := schedulingcorev1.FindMatchingUntoleratedTaint(
		klog.Background(),
		node.Spec.Taints,
		podSpec.Tolerations,
		doNotScheduleTaintsFilterFunc,
		false, // enableComparisonOperators
	)

	if hasUntoleratedTaint {
		slog.Debug("DaemonSet doesn't tolerate node taints",
			"daemonset", ds.Name, "node", node.Name)

		return false
	}

	return true
}

// doNotScheduleTaintsFilterFunc filters taints that prevent scheduling
func doNotScheduleTaintsFilterFunc(taint *v1.Taint) bool {
	return taint.Effect == v1.TaintEffectNoSchedule || taint.Effect == v1.TaintEffectNoExecute
}
