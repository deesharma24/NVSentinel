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
package controller

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nvidia/nvsentinel/health-monitors/kubernetes-object-monitor/pkg/annotations"
	"github.com/nvidia/nvsentinel/health-monitors/kubernetes-object-monitor/pkg/config"
	"github.com/nvidia/nvsentinel/health-monitors/kubernetes-object-monitor/pkg/metrics"
	"github.com/nvidia/nvsentinel/health-monitors/kubernetes-object-monitor/pkg/owner"
	"github.com/nvidia/nvsentinel/health-monitors/kubernetes-object-monitor/pkg/policy"
)

type HealthEventPublisher interface {
	PublishHealthEvent(
		ctx context.Context,
		policy *config.Policy,
		nodeName string,
		isHealthy bool,
		resourceInfo *config.ResourceInfo,
	) error
}

// matchStateInfo stores additional information about a match for owner-level tracking
type matchStateInfo struct {
	NodeName  string
	OwnerKind string
	OwnerName string
}

type ResourceReconciler struct {
	client.Client
	evaluator     *policy.Evaluator
	publisher     HealthEventPublisher
	annotationMgr *annotations.Manager
	ownerResolver *owner.Resolver
	policies      []config.Policy
	gvk           schema.GroupVersionKind
	// matchStates maps stateKey -> nodeName (for backward compatibility)
	matchStates map[string]string
	// matchStateInfos maps stateKey -> matchStateInfo (for owner-level tracking)
	matchStateInfos map[string]matchStateInfo
	matchStatesMu   sync.RWMutex
}

func NewResourceReconciler(
	c client.Client,
	evaluator *policy.Evaluator,
	pub HealthEventPublisher,
	annotationMgr *annotations.Manager,
	policies []config.Policy,
	gvk schema.GroupVersionKind,
) *ResourceReconciler {
	return &ResourceReconciler{
		Client:          c,
		evaluator:       evaluator,
		publisher:       pub,
		annotationMgr:   annotationMgr,
		ownerResolver:   owner.NewResolver(c),
		policies:        policies,
		gvk:             gvk,
		matchStates:     make(map[string]string),
		matchStateInfos: make(map[string]matchStateInfo),
	}
}

func (r *ResourceReconciler) LoadState(ctx context.Context) error {
	allMatches, err := r.annotationMgr.LoadAllMatches(ctx)
	if err != nil {
		return fmt.Errorf("failed to load match state: %w", err)
	}

	r.matchStatesMu.Lock()
	defer r.matchStatesMu.Unlock()

	maps.Copy(r.matchStates, allMatches)

	slog.Info("Loaded policy match state from annotations", "gvk", r.gvk.String(), "matches", len(allMatches))

	return nil
}

func (r *ResourceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(r.gvk)

	if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
		return r.handleGetError(ctx, err, req)
	}

	for _, p := range r.policies {
		if !p.Enabled {
			continue
		}

		if err := r.reconcilePolicy(ctx, &p, obj); err != nil {
			slog.Error("Failed to reconcile policy", "policy", p.Name, "resource", req.NamespacedName, "error", err)
			metrics.ReconciliationErrors.WithLabelValues(r.gvk.Kind, "policy_reconcile_error").Inc()
		}
	}

	return ctrl.Result{}, nil
}

func (r *ResourceReconciler) handleGetError(ctx context.Context, err error, req ctrl.Request) (ctrl.Result, error) {
	if client.IgnoreNotFound(err) == nil {
		r.cleanupDeletedResource(ctx, req)
		return ctrl.Result{}, nil
	}

	metrics.ReconciliationErrors.WithLabelValues(r.gvk.Kind, "get_resource_error").Inc()

	return ctrl.Result{}, fmt.Errorf("failed to get resource: %w", err)
}

func (r *ResourceReconciler) cleanupDeletedResource(ctx context.Context, req ctrl.Request) {
	slog.Info("Cleaning up deleted resource", "resource", req.NamespacedName)

	for _, p := range r.policies {
		if !p.Enabled {
			continue
		}

		if p.GetTrackingLevel() == config.TrackingLevelOwner {
			r.cleanupDeletedResourceOwnerLevel(ctx, req, &p)
		} else {
			r.cleanupDeletedResourceResourceLevel(ctx, req, &p)
		}
	}
}

// cleanupDeletedResourceResourceLevel handles cleanup for resource-level tracking (original behavior)
func (r *ResourceReconciler) cleanupDeletedResourceResourceLevel(
	ctx context.Context,
	req ctrl.Request,
	p *config.Policy,
) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(r.gvk)
	obj.SetNamespace(req.Namespace)
	obj.SetName(req.Name)

	stateKey := r.getStateKeyForResource(p, obj)

	r.matchStatesMu.RLock()
	nodeName, wasMatched := r.matchStates[stateKey]
	r.matchStatesMu.RUnlock()

	if wasMatched {
		// For Node resources, the deleted resource is the node itself.
		// In this case, there's no point publishing a healthy event or trying to
		// remove annotations - the node is gone.
		isNodeResource := r.gvk.Kind == "Node" && nodeName == req.Name
		if isNodeResource {
			slog.Info("Node deleted, cleaning up internal state without publishing",
				"policy", p.Name, "node", nodeName)

			r.matchStatesMu.Lock()
			delete(r.matchStates, stateKey)
			r.matchStatesMu.Unlock()

			return
		}

		resourceInfo := &config.ResourceInfo{
			Kind:      r.gvk.Kind,
			Namespace: req.Namespace,
			Name:      req.Name,
		}

		if err := r.publisher.PublishHealthEvent(ctx, p, nodeName, true, resourceInfo); err != nil {
			slog.Error("Failed to publish healthy event for deleted resource",
				"policy", p.Name,
				"resource", req.NamespacedName,
				"node", nodeName,
				"error", err)
			metrics.HealthEventsPublishErrors.WithLabelValues(p.Name, "grpc_error").Inc()
		}

		slog.Debug("Removing match state for deleted resource",
			"resource", req.NamespacedName,
			"node", nodeName,
			"policy", p.Name,
			"stateKey", stateKey)

		r.matchStatesMu.Lock()
		delete(r.matchStates, stateKey)
		r.matchStatesMu.Unlock()

		if err := r.annotationMgr.RemoveMatch(ctx, nodeName, stateKey); err != nil {
			slog.Error("Failed to remove match state from annotation", "node", nodeName, "stateKey", stateKey, "error", err)
		}
	}
}

// cleanupDeletedResourceOwnerLevel handles cleanup for owner-level tracking.
// It checks if the DaemonSet owner still exists and targets the node before deciding to uncordon.
func (r *ResourceReconciler) cleanupDeletedResourceOwnerLevel(
	ctx context.Context,
	req ctrl.Request,
	p *config.Policy,
) {
	// Find all matching state entries for this policy and namespace
	// State key format for owner-level: policyName/namespace/ownerKind/ownerName/nodeName
	prefix := fmt.Sprintf("%s/%s/", p.Name, req.Namespace)

	r.matchStatesMu.RLock()

	matches := make(map[string]matchStateInfo)

	for stateKey, info := range r.matchStateInfos {
		if strings.HasPrefix(stateKey, prefix) {
			matches[stateKey] = info
		}
	}

	r.matchStatesMu.RUnlock()

	for stateKey, info := range matches {
		ownerInfo := &owner.Info{
			Kind:      info.OwnerKind,
			Name:      info.OwnerName,
			Namespace: req.Namespace,
		}

		ownerTargetsNode, nodeExists, err := r.ownerResolver.OwnerTargetsNode(ctx, ownerInfo, info.NodeName)
		if err != nil {
			slog.Error("Failed to check if owner targets node",
				"owner", info.OwnerName, "node", info.NodeName, "error", err)

			continue // On error, don't uncordon to be safe
		}

		if ownerTargetsNode {
			slog.Info("Owner still targets node, waiting for replacement pod",
				"policy", p.Name, "owner", info.OwnerName, "node", info.NodeName)

			continue
		}

		// If node was deleted, just clean up internal state without publishing
		if !nodeExists {
			slog.Info("Node deleted, cleaning up internal state without publishing",
				"policy", p.Name, "owner", info.OwnerName, "node", info.NodeName)

			r.matchStatesMu.Lock()
			delete(r.matchStates, stateKey)
			delete(r.matchStateInfos, stateKey)
			r.matchStatesMu.Unlock()

			continue
		}

		// Owner doesn't exist or no longer targets this node - uncordon
		slog.Info("Owner no longer targets node, publishing healthy event",
			"policy", p.Name, "owner", info.OwnerName, "node", info.NodeName)

		resourceInfo := &config.ResourceInfo{
			Kind:      info.OwnerKind,
			Namespace: req.Namespace,
			Name:      info.OwnerName,
		}

		if err := r.publisher.PublishHealthEvent(ctx, p, info.NodeName, true, resourceInfo); err != nil {
			slog.Error("Failed to publish healthy event", "policy", p.Name, "owner", info.OwnerName, "error", err)
			metrics.HealthEventsPublishErrors.WithLabelValues(p.Name, "grpc_error").Inc()

			continue
		}

		r.matchStatesMu.Lock()
		delete(r.matchStates, stateKey)
		delete(r.matchStateInfos, stateKey)
		r.matchStatesMu.Unlock()

		if err := r.annotationMgr.RemoveMatch(ctx, info.NodeName, stateKey); err != nil {
			slog.Error("Failed to remove match state from annotation", "node", info.NodeName, "stateKey", stateKey, "error", err)
		}
	}
}

func (r *ResourceReconciler) reconcilePolicy(
	ctx context.Context,
	p *config.Policy,
	obj *unstructured.Unstructured,
) error {
	matched, err := r.evaluator.EvaluatePredicate(ctx, p.Name, obj)
	if err != nil {
		metrics.PolicyEvaluationErrors.WithLabelValues(p.Name, "cel_error").Inc()

		return fmt.Errorf("predicate evaluation failed: %w", err)
	}

	nodeName, err := r.evaluator.EvaluateNodeAssociation(ctx, p.Name, obj)
	if err != nil {
		metrics.PolicyEvaluationErrors.WithLabelValues(p.Name, "node_association_error").Inc()

		return fmt.Errorf("node association evaluation failed: %w", err)
	}

	if nodeName == "" {
		nodeName = obj.GetName()
	}

	// Get owner info for owner-level tracking
	ownerInfo, skip := r.getOwnerInfoForTracking(p, obj)
	if skip {
		return nil
	}

	stateKey := r.getStateKey(p, obj, nodeName, ownerInfo)

	r.matchStatesMu.RLock()
	storedNodeName, wasMatched := r.matchStates[stateKey]
	r.matchStatesMu.RUnlock()

	// For owner-level tracking, use owner info in resourceInfo
	resourceInfo := r.buildResourceInfo(obj, ownerInfo)

	if matched && !wasMatched {
		return r.handleUnhealthyTransition(ctx, p, nodeName, stateKey, resourceInfo, ownerInfo)
	}

	if !matched && wasMatched {
		return r.handleHealthyTransition(ctx, p, storedNodeName, stateKey, resourceInfo)
	}

	return nil
}

// getOwnerInfoForTracking returns owner info for owner-level tracking.
// Returns (ownerInfo, shouldSkip) where shouldSkip is true if the resource should be skipped.
func (r *ResourceReconciler) getOwnerInfoForTracking(
	p *config.Policy,
	obj *unstructured.Unstructured,
) (*owner.Info, bool) {
	if p.GetTrackingLevel() != config.TrackingLevelOwner {
		return nil, false
	}

	ownerInfo := r.ownerResolver.GetControllerOwner(obj)

	// For owner-level tracking, only process resources owned by DaemonSets.
	// Skip resources without an owner or with non-DaemonSet owners (e.g., ReplicaSet, Job).
	// This ensures we only track pods that are expected to have replacements on the same node.
	if ownerInfo == nil {
		slog.Debug("Skipping resource without controller owner for owner-level tracking",
			"policy", p.Name, "resource", obj.GetName(), "namespace", obj.GetNamespace())

		return nil, true
	}

	if ownerInfo.Kind != "DaemonSet" {
		slog.Debug("Skipping resource with non-DaemonSet owner for owner-level tracking",
			"policy", p.Name, "resource", obj.GetName(), "ownerKind", ownerInfo.Kind, "ownerName", ownerInfo.Name)

		return nil, true
	}

	return ownerInfo, false
}

// buildResourceInfo creates ResourceInfo based on tracking level
func (r *ResourceReconciler) buildResourceInfo(
	obj *unstructured.Unstructured,
	ownerInfo *owner.Info,
) *config.ResourceInfo {
	if ownerInfo != nil {
		// For owner-level tracking, report the owner in the resource info
		return &config.ResourceInfo{
			Kind:      ownerInfo.Kind,
			Namespace: ownerInfo.Namespace,
			Name:      ownerInfo.Name,
		}
	}

	// For resource-level tracking, report the resource itself
	return &config.ResourceInfo{
		Kind:      r.gvk.Kind,
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}
}

func (r *ResourceReconciler) handleUnhealthyTransition(
	ctx context.Context,
	p *config.Policy,
	nodeName string,
	stateKey string,
	resourceInfo *config.ResourceInfo,
	ownerInfo *owner.Info,
) error {
	if err := r.publisher.PublishHealthEvent(ctx, p, nodeName, false, resourceInfo); err != nil {
		metrics.HealthEventsPublishErrors.WithLabelValues(p.Name, "grpc_error").Inc()
		return fmt.Errorf("failed to publish unhealthy event: %w", err)
	}

	r.matchStatesMu.Lock()
	r.matchStates[stateKey] = nodeName

	if ownerInfo != nil {
		r.matchStateInfos[stateKey] = matchStateInfo{
			NodeName:  nodeName,
			OwnerKind: ownerInfo.Kind,
			OwnerName: ownerInfo.Name,
		}
	}

	r.matchStatesMu.Unlock()

	if err := r.annotationMgr.AddMatch(ctx, nodeName, stateKey, nodeName); err != nil {
		slog.Error("Failed to persist match state to annotation", "node", nodeName, "stateKey", stateKey, "error", err)
	}

	metrics.PolicyMatches.WithLabelValues(p.Name, nodeName, r.gvk.Kind).Inc()

	return nil
}

func (r *ResourceReconciler) handleHealthyTransition(
	ctx context.Context,
	p *config.Policy,
	nodeName string,
	stateKey string,
	resourceInfo *config.ResourceInfo,
) error {
	if err := r.publisher.PublishHealthEvent(ctx, p, nodeName, true, resourceInfo); err != nil {
		metrics.HealthEventsPublishErrors.WithLabelValues(p.Name, "grpc_error").Inc()
		return fmt.Errorf("failed to publish healthy event: %w", err)
	}

	r.matchStatesMu.Lock()
	delete(r.matchStates, stateKey)
	delete(r.matchStateInfos, stateKey)
	r.matchStatesMu.Unlock()

	if err := r.annotationMgr.RemoveMatch(ctx, nodeName, stateKey); err != nil {
		slog.Error("Failed to remove match state from annotation", "node", nodeName, "stateKey", stateKey, "error", err)
	}

	return nil
}

// getStateKey generates the state key based on tracking level
// For resource-level: policyName/namespace/resourceName
// For owner-level: policyName/namespace/ownerKind/ownerName/nodeName
func (r *ResourceReconciler) getStateKey(
	p *config.Policy,
	obj *unstructured.Unstructured,
	nodeName string,
	ownerInfo *owner.Info,
) string {
	if p.GetTrackingLevel() == config.TrackingLevelOwner && ownerInfo != nil {
		// Owner-level tracking: include owner info and node name
		// This ensures the same owner on different nodes has different state keys
		return fmt.Sprintf("%s/%s/%s/%s/%s", p.Name, obj.GetNamespace(), ownerInfo.Kind, ownerInfo.Name, nodeName)
	}

	// Resource-level tracking (default)
	return r.getStateKeyForResource(p, obj)
}

// getStateKeyForResource generates state key for resource-level tracking
func (r *ResourceReconciler) getStateKeyForResource(p *config.Policy, obj *unstructured.Unstructured) string {
	if obj.GetNamespace() != "" {
		return fmt.Sprintf("%s/%s/%s", p.Name, obj.GetNamespace(), obj.GetName())
	}

	return fmt.Sprintf("%s/%s", p.Name, obj.GetName())
}
