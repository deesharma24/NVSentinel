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
package config

type Config struct {
	Policies []Policy `toml:"policies"`
}

type Policy struct {
	Name            string           `toml:"name"`
	Enabled         bool             `toml:"enabled"`
	Resource        ResourceSpec     `toml:"resource"`
	Predicate       PredicateSpec    `toml:"predicate"`
	NodeAssociation *AssociationSpec `toml:"nodeAssociation,omitempty"`
	Tracking        *TrackingSpec    `toml:"tracking,omitempty"`
	HealthEvent     HealthEventSpec  `toml:"healthEvent"`
}

// TrackingLevel defines how resources are tracked for state management
type TrackingLevel string

const (
	// TrackingLevelResource tracks by individual resource name (default)
	// stateKey = policyName/namespace/resourceName
	TrackingLevelResource TrackingLevel = "resource"

	// TrackingLevelOwner tracks by owner reference (for DaemonSet/ReplicaSet pods)
	// stateKey = policyName/namespace/ownerKind/ownerName/nodeName
	// When a pod is deleted, the node stays cordoned until:
	// - A healthy replacement pod from the same owner appears on the node, OR
	// - The owner is deleted, OR
	// - The owner no longer targets this node (nodeSelector/taint change)
	TrackingLevelOwner TrackingLevel = "owner"
)

// TrackingSpec configures how resources are tracked for cordon/uncordon decisions
type TrackingSpec struct {
	// Level specifies the tracking granularity: "resource" (default) or "owner"
	Level TrackingLevel `toml:"level"`
}

type ResourceSpec struct {
	Group   string `toml:"group"`
	Version string `toml:"version"`
	Kind    string `toml:"kind"`
}

type PredicateSpec struct {
	Expression string `toml:"expression"`
}

type AssociationSpec struct {
	Expression string `toml:"expression"`
}

type HealthEventSpec struct {
	ComponentClass    string   `toml:"componentClass"`
	IsFatal           bool     `toml:"isFatal"`
	Message           string   `toml:"message"`
	RecommendedAction string   `toml:"recommendedAction"`
	ErrorCode         []string `toml:"errorCode"`
	// override the processing strategy for the policy
	ProcessingStrategy string `toml:"processingStrategy"`
}

func (r *ResourceSpec) GVK() string {
	if r.Group == "" {
		return r.Version + "/" + r.Kind
	}

	return r.Group + "/" + r.Version + "/" + r.Kind
}

// GetTrackingLevel returns the tracking level for the policy, defaulting to resource-level tracking
func (p *Policy) GetTrackingLevel() TrackingLevel {
	if p.Tracking == nil || p.Tracking.Level == "" {
		return TrackingLevelResource
	}

	return p.Tracking.Level
}

// ResourceInfo contains the metadata needed to identify a resource in health events.
// This is used to populate the entitiesImpacted field, which allows fault-quarantine
// to track each resource individually.
type ResourceInfo struct {
	Kind      string
	Namespace string
	Name      string
}
