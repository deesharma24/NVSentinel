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

package owner

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestGetControllerOwner(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	resolver := NewResolver(client)

	tests := []struct {
		name          string
		ownerRefs     []metav1.OwnerReference
		expectedOwner *Info
		expectedNil   bool
	}{
		{
			name:        "no owner references",
			ownerRefs:   nil,
			expectedNil: true,
		},
		{
			name: "has controller owner - DaemonSet",
			ownerRefs: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "DaemonSet",
					Name:       "nvidia-driver",
					Controller: boolPtr(true),
				},
			},
			expectedOwner: &Info{
				Kind:      "DaemonSet",
				Name:      "nvidia-driver",
				Namespace: "gpu-operator",
			},
		},
		{
			name: "has non-controller owner",
			ownerRefs: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "DaemonSet",
					Name:       "nvidia-driver",
					Controller: boolPtr(false),
				},
			},
			expectedNil: true,
		},
		{
			name: "multiple owners, one is controller",
			ownerRefs: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Node",
					Name:       "node-1",
					Controller: boolPtr(false),
				},
				{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       "my-deployment-abc123",
					Controller: boolPtr(true),
				},
			},
			expectedOwner: &Info{
				Kind:      "ReplicaSet",
				Name:      "my-deployment-abc123",
				Namespace: "gpu-operator",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := &unstructured.Unstructured{}
			obj.SetNamespace("gpu-operator")
			obj.SetName("test-pod")
			obj.SetOwnerReferences(tt.ownerRefs)

			result := resolver.GetControllerOwner(obj)

			if tt.expectedNil {
				assert.Nil(t, result)
			} else {
				require.NotNil(t, result)
				assert.Equal(t, tt.expectedOwner.Kind, result.Kind)
				assert.Equal(t, tt.expectedOwner.Name, result.Name)
				assert.Equal(t, tt.expectedOwner.Namespace, result.Namespace)
			}
		})
	}
}

func TestDaemonSetTargetsNode(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	tests := []struct {
		name           string
		daemonSet      *appsv1.DaemonSet
		node           *v1.Node
		expectedResult bool
		dsNotFound     bool
		nodeNotFound   bool
	}{
		{
			name: "DaemonSet targets all nodes",
			daemonSet: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "nvidia-driver",
					Namespace: "gpu-operator",
				},
				Spec: appsv1.DaemonSetSpec{
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{},
					},
				},
			},
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-1",
				},
			},
			expectedResult: true,
		},
		{
			name: "DaemonSet with nodeSelector matches node",
			daemonSet: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "nvidia-driver",
					Namespace: "gpu-operator",
				},
				Spec: appsv1.DaemonSetSpec{
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							NodeSelector: map[string]string{
								"nvidia.com/gpu": "true",
							},
						},
					},
				},
			},
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-1",
					Labels: map[string]string{
						"nvidia.com/gpu": "true",
					},
				},
			},
			expectedResult: true,
		},
		{
			name: "DaemonSet with nodeSelector does not match node",
			daemonSet: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "nvidia-driver",
					Namespace: "gpu-operator",
				},
				Spec: appsv1.DaemonSetSpec{
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							NodeSelector: map[string]string{
								"nvidia.com/gpu": "true",
							},
						},
					},
				},
			},
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "node-1",
					Labels: map[string]string{},
				},
			},
			expectedResult: false,
		},
		{
			name: "DaemonSet tolerates node taint",
			daemonSet: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "nvidia-driver",
					Namespace: "gpu-operator",
				},
				Spec: appsv1.DaemonSetSpec{
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							Tolerations: []v1.Toleration{
								{
									Key:      "nvidia.com/gpu",
									Operator: v1.TolerationOpExists,
									Effect:   v1.TaintEffectNoSchedule,
								},
							},
						},
					},
				},
			},
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-1",
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    "nvidia.com/gpu",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			expectedResult: true,
		},
		{
			name: "DaemonSet does not tolerate node taint",
			daemonSet: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "nvidia-driver",
					Namespace: "gpu-operator",
				},
				Spec: appsv1.DaemonSetSpec{
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							Tolerations: []v1.Toleration{},
						},
					},
				},
			},
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-1",
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    "nvidia.com/gpu",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			expectedResult: false,
		},
		{
			name: "DaemonSet with node affinity matches node",
			daemonSet: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "nvidia-driver",
					Namespace: "gpu-operator",
				},
				Spec: appsv1.DaemonSetSpec{
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							Affinity: &v1.Affinity{
								NodeAffinity: &v1.NodeAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
										NodeSelectorTerms: []v1.NodeSelectorTerm{
											{
												MatchExpressions: []v1.NodeSelectorRequirement{
													{
														Key:      "zone",
														Operator: v1.NodeSelectorOpIn,
														Values:   []string{"us-west-1a", "us-west-1b"},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-1",
					Labels: map[string]string{
						"zone": "us-west-1a",
					},
				},
			},
			expectedResult: true,
		},
		{
			name: "DaemonSet with node affinity does not match node",
			daemonSet: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "nvidia-driver",
					Namespace: "gpu-operator",
				},
				Spec: appsv1.DaemonSetSpec{
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							Affinity: &v1.Affinity{
								NodeAffinity: &v1.NodeAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
										NodeSelectorTerms: []v1.NodeSelectorTerm{
											{
												MatchExpressions: []v1.NodeSelectorRequirement{
													{
														Key:      "zone",
														Operator: v1.NodeSelectorOpIn,
														Values:   []string{"us-west-1a", "us-west-1b"},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-1",
					Labels: map[string]string{
						"zone": "us-east-1a",
					},
				},
			},
			expectedResult: false,
		},
		{
			name:           "DaemonSet not found",
			daemonSet:      nil,
			dsNotFound:     true,
			node:           &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
			expectedResult: false,
		},
		{
			name: "Node not found",
			daemonSet: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "nvidia-driver",
					Namespace: "gpu-operator",
				},
			},
			nodeNotFound:   true,
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)

			if tt.daemonSet != nil && !tt.dsNotFound {
				builder = builder.WithObjects(tt.daemonSet)
			}
			if tt.node != nil && !tt.nodeNotFound {
				builder = builder.WithObjects(tt.node)
			}

			client := builder.Build()
			resolver := NewResolver(client)

			ctx := context.Background()
			nodeName := "node-1"
			if tt.node != nil {
				nodeName = tt.node.Name
			}

			result, _, err := resolver.daemonSetTargetsNode(ctx, "gpu-operator", "nvidia-driver", nodeName)

			require.NoError(t, err)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestOwnerTargetsNode(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	// This test focuses on edge cases and owner type filtering.
	// Detailed DaemonSet scheduling logic is tested in TestDaemonSetTargetsNode.
	tests := []struct {
		name               string
		ownerInfo          *Info
		objects            []runtime.Object
		expectedResult     bool
		expectedNodeExists bool
	}{
		{
			name:               "nil owner info",
			ownerInfo:          nil,
			expectedResult:     false,
			expectedNodeExists: true, // Assume node exists when owner is nil
		},
		{
			name: "DaemonSet owner deleted - should return false, node exists",
			ownerInfo: &Info{
				Kind:      "DaemonSet",
				Name:      "nvidia-driver",
				Namespace: "gpu-operator",
			},
			objects: []runtime.Object{
				&v1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node-1",
					},
				},
			},
			expectedResult:     false,
			expectedNodeExists: true,
		},
		{
			name: "DaemonSet owner exists but node deleted - should return false, node not exists",
			ownerInfo: &Info{
				Kind:      "DaemonSet",
				Name:      "nvidia-driver",
				Namespace: "gpu-operator",
			},
			objects: []runtime.Object{
				&appsv1.DaemonSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "nvidia-driver",
						Namespace: "gpu-operator",
					},
				},
				// No node object - simulates deleted node
			},
			expectedResult:     false,
			expectedNodeExists: false,
		},
		{
			name: "ReplicaSet owner - not supported, returns false",
			ownerInfo: &Info{
				Kind:      "ReplicaSet",
				Name:      "my-deployment-abc123",
				Namespace: "default",
			},
			expectedResult:     false,
			expectedNodeExists: true, // Assume node exists for unsupported owner types
		},
		{
			name: "StatefulSet owner - not supported, returns false",
			ownerInfo: &Info{
				Kind:      "StatefulSet",
				Name:      "my-statefulset",
				Namespace: "default",
			},
			expectedResult:     false,
			expectedNodeExists: true,
		},
		{
			name: "Job owner - not supported, returns false",
			ownerInfo: &Info{
				Kind:      "Job",
				Name:      "my-job",
				Namespace: "default",
			},
			expectedResult:     false,
			expectedNodeExists: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if len(tt.objects) > 0 {
				builder = builder.WithRuntimeObjects(tt.objects...)
			}

			client := builder.Build()
			resolver := NewResolver(client)

			ctx := context.Background()
			result, nodeExists, err := resolver.OwnerTargetsNode(ctx, tt.ownerInfo, "node-1")

			require.NoError(t, err)
			assert.Equal(t, tt.expectedResult, result)
			assert.Equal(t, tt.expectedNodeExists, nodeExists)
		})
	}
}

func boolPtr(b bool) *bool {
	return &b
}
