//go:build arm64_group
// +build arm64_group

// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tests

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"tests/helpers"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

type k8sObjectMonitorContextKey int

const (
	k8sMonitorKeyNodeName     k8sObjectMonitorContextKey = iota
	k8sMonitorKeyOriginalArgs k8sObjectMonitorContextKey = iota

	annotationKey            = "nvsentinel.nvidia.com/k8s-object-monitor-policy-matches"
	testConditionType        = "TestCondition"
	gpuOperatorNamespace     = "gpu-operator"
	gpuOperatorPodPolicyName = "gpu-operator-pod-health"

	// policyTimeoutWait is the time to wait for policy timeout to elapse.
	// Calculated as: policy delay (30s) + resync period (30s) + buffer (10s)
	policyTimeoutWait = 70 * time.Second
)

func TestKubernetesObjectMonitor(t *testing.T) {
	feature := features.New("Kubernetes Object Monitor - Node Not Ready Detection").
		WithLabel("suite", "kubernetes-object-monitor").
		WithLabel("component", "node-monitoring")

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		nodeList := &v1.NodeList{}
		err = client.Resources().List(ctx, nodeList)
		require.NoError(t, err)

		var testNodeName string
		for _, node := range nodeList.Items {
			if node.Labels["type"] != "kwok" {
				testNodeName = node.Name
				break
			}
		}
		require.NotEmpty(t, testNodeName, "no worker node found in cluster")
		t.Logf("Using test node: %s", testNodeName)

		return context.WithValue(ctx, k8sMonitorKeyNodeName, testNodeName)
	})

	feature.Assess("Node NotReady triggers health event", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		nodeName := ctx.Value(k8sMonitorKeyNodeName).(string)
		t.Logf("Setting TestCondition to False on node %s", nodeName)

		helpers.SetNodeConditionStatus(ctx, t, client, nodeName, v1.NodeConditionType(testConditionType), v1.ConditionFalse)

		t.Log("Waiting for policy match annotation on node")
		require.Eventually(t, func() bool {
			node, err := helpers.GetNodeByName(ctx, client, nodeName)
			if err != nil {
				t.Logf("Failed to get node: %v", err)
				return false
			}

			annotation, exists := node.Annotations[annotationKey]
			if !exists {
				return false
			}

			t.Logf("Found policy match annotation: %s", annotation)
			return true
		}, helpers.EventuallyWaitTimeout, helpers.WaitInterval)

		helpers.WaitForNodeEvent(ctx, t, client, nodeName, v1.Event{
			Type:   "node-test-condition",
			Reason: "node-test-conditionIsNotHealthy",
		})

		return ctx
	})

	feature.Assess("Node Ready recovery clears annotation", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		nodeName := ctx.Value(k8sMonitorKeyNodeName).(string)
		t.Logf("Setting TestCondition to True on node %s", nodeName)

		helpers.SetNodeConditionStatus(ctx, t, client, nodeName, v1.NodeConditionType(testConditionType), v1.ConditionTrue)

		t.Log("Waiting for policy match annotation to be cleared")
		require.Eventually(t, func() bool {
			node, err := helpers.GetNodeByName(ctx, client, nodeName)
			if err != nil {
				t.Logf("Failed to get node: %v", err)
				return false
			}

			annotation, exists := node.Annotations[annotationKey]
			if exists && annotation != "" {
				t.Logf("Annotation still exists: %s", annotation)
				return false
			}

			return true
		}, helpers.EventuallyWaitTimeout, helpers.WaitInterval)

		return ctx
	})

	testEnv.Test(t, feature.Feature())
}

func TestKubernetesObjectMonitorWithStoreOnlyStrategy(t *testing.T) {
	feature := features.New("Kubernetes Object Monitor with STORE_ONLY strategy - Node Not Ready Detection").
		WithLabel("suite", "kubernetes-object-monitor").
		WithLabel("component", "node-monitoring")

	var testCtx *helpers.KubernetesObjectMonitorTestContext

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		// Find the test node first
		nodeList := &v1.NodeList{}
		err = client.Resources().List(ctx, nodeList)
		require.NoError(t, err)

		var testNodeName string
		for _, node := range nodeList.Items {
			if node.Labels["type"] != "kwok" {
				testNodeName = node.Name
				break
			}
		}
		require.NotEmpty(t, testNodeName, "no worker node found in cluster")
		t.Logf("Using test node: %s", testNodeName)

		err = helpers.DeleteExistingNodeEvents(ctx, t, client, testNodeName, "node-test-condition", "node-test-conditionIsNotHealthy")
		require.NoError(t, err)

		originalArgs, err := helpers.SetDeploymentArgs(ctx, t, client, helpers.K8S_DEPLOYMENT_NAME, helpers.NVSentinelNamespace, helpers.K8S_CONTAINER_NAME, map[string]string{
			"--processing-strategy": "STORE_ONLY",
		})
		require.NoError(t, err)

		testCtx = &helpers.KubernetesObjectMonitorTestContext{
			NodeName: testNodeName,
		}

		ctx = context.WithValue(ctx, k8sMonitorKeyOriginalArgs, originalArgs)

		helpers.WaitForDeploymentRollout(ctx, t, client, helpers.K8S_DEPLOYMENT_NAME, helpers.NVSentinelNamespace)

		return context.WithValue(ctx, k8sMonitorKeyNodeName, testNodeName)
	})

	feature.Assess("Node NotReady triggers health event with STORE_ONLY strategy", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		nodeName := ctx.Value(k8sMonitorKeyNodeName).(string)
		t.Logf("Setting TestCondition to False on node %s", nodeName)

		helpers.SetNodeConditionStatus(ctx, t, client, nodeName, v1.NodeConditionType(testConditionType), v1.ConditionFalse)

		t.Log("Waiting for policy match annotation on node")
		require.Eventually(t, func() bool {
			node, err := helpers.GetNodeByName(ctx, client, nodeName)
			if err != nil {
				t.Logf("Failed to get node: %v", err)
				return false
			}

			annotation, exists := node.Annotations[annotationKey]
			if !exists {
				return false
			}

			t.Logf("Found policy match annotation: %s", annotation)
			return true
		}, helpers.EventuallyWaitTimeout, helpers.WaitInterval)

		t.Log("Check node event is not created")
		helpers.EnsureNodeEventNotPresent(ctx, t, client, nodeName, "node-test-condition", "node-test-conditionIsNotHealthy")

		return ctx
	})

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		originalArgs := ctx.Value(k8sMonitorKeyOriginalArgs).([]string)
		client, err := c.NewClient()
		require.NoError(t, err)

		t.Logf("Setting TestCondition to True on node %s", testCtx.NodeName)

		helpers.SetNodeConditionStatus(ctx, t, client, testCtx.NodeName, v1.NodeConditionType(testConditionType), v1.ConditionTrue)

		helpers.TeardownKubernetesObjectMonitor(ctx, t, c, testCtx.ConfigMapBackup, originalArgs)

		return ctx
	})

	testEnv.Test(t, feature.Feature())
}

func TestKubernetesObjectMonitorWithRuleOverride(t *testing.T) {
	feature := features.New("Kubernetes Object Monitor with Rule Override for processingStrategy=STORE_ONLY - Node Not Ready Detection").
		WithLabel("suite", "kubernetes-object-monitor").
		WithLabel("component", "node-monitoring")

	var testCtx *helpers.KubernetesObjectMonitorTestContext

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		// Find the test node first
		nodeList := &v1.NodeList{}
		err = client.Resources().List(ctx, nodeList)
		require.NoError(t, err)

		var testNodeName string
		for _, node := range nodeList.Items {
			if node.Labels["type"] != "kwok" {
				testNodeName = node.Name
				break
			}
		}
		require.NotEmpty(t, testNodeName, "no worker node found in cluster")
		t.Logf("Using test node: %s", testNodeName)

		err = helpers.DeleteExistingNodeEvents(ctx, t, client, testNodeName, "node-test-condition", "node-test-conditionIsNotHealthy")
		require.NoError(t, err)

		t.Log("Backing up current configmap")

		backupData, err := helpers.BackupConfigMap(ctx, client, "kubernetes-object-monitor", helpers.NVSentinelNamespace)
		require.NoError(t, err)
		t.Log("Backup created in memory")

		testCtx = &helpers.KubernetesObjectMonitorTestContext{
			NodeName:        testNodeName,
			ConfigMapBackup: backupData,
		}

		helpers.UpdateKubernetesObjectMonitorConfigMap(ctx, t, client, "data/k8s-rule-override.yaml", "kubernetes-object-monitor")

		return context.WithValue(ctx, k8sMonitorKeyNodeName, testNodeName)
	})

	feature.Assess("Node NotReady triggers health event with STORE_ONLY strategy", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		nodeName := ctx.Value(k8sMonitorKeyNodeName).(string)
		t.Logf("Setting TestCondition to False on node %s", nodeName)

		helpers.SetNodeConditionStatus(ctx, t, client, nodeName, v1.NodeConditionType(testConditionType), v1.ConditionFalse)

		t.Log("Waiting for policy match annotation on node")
		require.Eventually(t, func() bool {
			node, err := helpers.GetNodeByName(ctx, client, nodeName)
			if err != nil {
				t.Logf("Failed to get node: %v", err)
				return false
			}

			annotation, exists := node.Annotations[annotationKey]
			if !exists {
				return false
			}

			t.Logf("Found policy match annotation: %s", annotation)
			return true
		}, helpers.EventuallyWaitTimeout, helpers.WaitInterval)

		t.Log("Check node event is not created")
		helpers.EnsureNodeEventNotPresent(ctx, t, client, nodeName, "node-test-condition", "node-test-conditionIsNotHealthy")

		return ctx
	})

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		t.Logf("Setting TestCondition to True on node %s", testCtx.NodeName)

		helpers.SetNodeConditionStatus(ctx, t, client, testCtx.NodeName, v1.NodeConditionType(testConditionType), v1.ConditionTrue)

		t.Log("Restoring kubernetes-object-monitor state")

		helpers.TeardownKubernetesObjectMonitor(ctx, t, c, testCtx.ConfigMapBackup, nil)

		return ctx
	})

	testEnv.Test(t, feature.Feature())
}

// daemonSetOwnerTestContext holds context for DaemonSet owner-level tracking tests
type daemonSetOwnerTestContext struct {
	NodeName       string
	DaemonSetName  string
	DaemonSetName2 string // For multiple DaemonSet tests
	Namespace      string
}

// TestKubernetesObjectMonitorDaemonSetOwnerTracking is a comprehensive test for owner-level
// tracking of DaemonSet pods. It tests multiple scenarios in a single test to minimize
// the total test time (only one 2m15s policy timeout wait).
//
// Scenarios tested:
// 1. Multiple DaemonSet failures on same node - both tracked separately
// 2. Pod deletion keeps node cordoned (waiting for replacement)
// 3. First DaemonSet recovery does NOT uncordon (second still failing)
// 4. Second DaemonSet recovery uncordons node (all resolved)
// 5. DaemonSet deletion uncordons node immediately
//
// Total test time: ~5 minutes (2m15s wait + verification steps)
func TestKubernetesObjectMonitorDaemonSetOwnerTracking(t *testing.T) {
	feature := features.New("Kubernetes Object Monitor - DaemonSet Owner-Level Tracking").
		WithLabel("suite", "kubernetes-object-monitor").
		WithLabel("component", "owner-tracking")

	var testCtx *daemonSetOwnerTestContext

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		// Find a real worker node (non-kwok, non-control-plane)
		// We need a real node that can actually run pods (not kwok simulated nodes)
		nodeList := &v1.NodeList{}
		err = client.Resources().List(ctx, nodeList)
		require.NoError(t, err)

		var testNodeName string
		for _, node := range nodeList.Items {
			// Skip kwok simulated nodes - they can't run real pods
			if node.Labels["type"] == "kwok" {
				continue
			}
			// Skip control-plane nodes
			_, isControlPlane := node.Labels["node-role.kubernetes.io/control-plane"]
			if isControlPlane {
				continue
			}
			// Found a real worker node
			testNodeName = node.Name
			break
		}
		require.NotEmpty(t, testNodeName, "no real worker node found in cluster")
		t.Logf("Using test worker node: %s", testNodeName)

		// Ensure gpu-operator namespace exists
		err = helpers.CreateNamespace(ctx, client, gpuOperatorNamespace)
		require.NoError(t, err, "failed to create namespace %s", gpuOperatorNamespace)

		testCtx = &daemonSetOwnerTestContext{
			NodeName:       testNodeName,
			DaemonSetName:  "test-ds-owner-1",
			DaemonSetName2: "test-ds-owner-2",
			Namespace:      gpuOperatorNamespace,
		}

		// Clean up any leftover resources from previous runs
		cleanupDaemonSet(ctx, t, client, testCtx.Namespace, testCtx.DaemonSetName)
		cleanupDaemonSet(ctx, t, client, testCtx.Namespace, testCtx.DaemonSetName2)

		return ctx
	})

	feature.Assess("Create two failing DaemonSets and wait for policy timeout", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		// Create BOTH failing DaemonSets at the same time to share the policy timeout wait
		ds1 := createTestDaemonSetWithUniqueSelector(testCtx.DaemonSetName, testCtx.Namespace, testCtx.NodeName, true)
		err = client.Resources().Create(ctx, ds1)
		require.NoError(t, err)
		t.Logf("Created first DaemonSet %s", testCtx.DaemonSetName)

		ds2 := createTestDaemonSetWithUniqueSelector(testCtx.DaemonSetName2, testCtx.Namespace, testCtx.NodeName, true)
		err = client.Resources().Create(ctx, ds2)
		require.NoError(t, err)
		t.Logf("Created second DaemonSet %s", testCtx.DaemonSetName2)

		// Wait for both pods to be created
		t.Log("Waiting for DaemonSet pods to be created")
		require.Eventually(t, func() bool {
			pods1, _ := listDaemonSetPods(ctx, client, testCtx.Namespace, testCtx.DaemonSetName)
			pods2, _ := listDaemonSetPods(ctx, client, testCtx.Namespace, testCtx.DaemonSetName2)
			if len(pods1) > 0 && len(pods2) > 0 {
				t.Logf("Both DaemonSet pods created: %s (phase: %s), %s (phase: %s)",
					pods1[0].Name, pods1[0].Status.Phase,
					pods2[0].Name, pods2[0].Status.Phase)
				return true
			}
			return false
		}, helpers.EventuallyWaitTimeout, helpers.WaitInterval)

		t.Log("Waiting for policy timeout to elapse (single wait for all DaemonSets)")
		time.Sleep(policyTimeoutWait)

		return ctx
	})

	feature.Assess("Both DaemonSets tracked in annotation and node cordoned", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		// Verify annotation contains BOTH DaemonSets
		t.Log("Verifying both DaemonSets appear in annotation")
		require.Eventually(t, func() bool {
			has1 := checkDaemonSetInAnnotation(ctx, t, client, testCtx.NodeName, testCtx.DaemonSetName)
			has2 := checkDaemonSetInAnnotation(ctx, t, client, testCtx.NodeName, testCtx.DaemonSetName2)
			t.Logf("Annotation contains DS1=%v, DS2=%v", has1, has2)
			return has1 && has2
		}, helpers.EventuallyWaitTimeout, helpers.WaitInterval)

		// Verify node is cordoned
		helpers.AssertQuarantineState(ctx, t, client, testCtx.NodeName, helpers.QuarantineAssertion{
			ExpectCordoned:   true,
			ExpectAnnotation: true,
		})
		t.Log("Node correctly cordoned with both DaemonSet failures tracked")

		return ctx
	})

	feature.Assess("Pod deletion keeps node cordoned (owner-level tracking)", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		// Get current pod name for DS1
		pods, err := listDaemonSetPods(ctx, client, testCtx.Namespace, testCtx.DaemonSetName)
		require.NoError(t, err)
		require.NotEmpty(t, pods, "DaemonSet should have at least one pod")
		oldPodName := pods[0].Name

		// Delete the pod - DaemonSet controller will create a replacement
		t.Logf("Deleting DaemonSet pod %s to test owner-level tracking", oldPodName)
		err = helpers.DeletePod(ctx, t, client, testCtx.Namespace, oldPodName, true)
		require.NoError(t, err)

		// Wait for replacement pod to be created
		t.Log("Waiting for replacement pod")
		require.Eventually(t, func() bool {
			pods, err := listDaemonSetPods(ctx, client, testCtx.Namespace, testCtx.DaemonSetName)
			if err != nil || len(pods) == 0 {
				return false
			}
			if pods[0].Name != oldPodName {
				t.Logf("Found replacement pod: %s", pods[0].Name)
				return true
			}
			return false
		}, helpers.EventuallyWaitTimeout, helpers.WaitInterval)

		// Node should STILL be cordoned (owner-level tracking prevents premature uncordon)
		time.Sleep(5 * time.Second)
		helpers.AssertQuarantineState(ctx, t, client, testCtx.NodeName, helpers.QuarantineAssertion{
			ExpectCordoned:   true,
			ExpectAnnotation: true,
		})
		t.Log("Node correctly remains cordoned after pod deletion (owner-level tracking working)")

		return ctx
	})

	feature.Assess("First DaemonSet recovery does NOT uncordon node", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		// Update first DaemonSet to healthy (remove init blocker)
		t.Logf("Updating first DaemonSet %s to healthy", testCtx.DaemonSetName)
		err = updateDaemonSetToHealthy(ctx, client, testCtx.Namespace, testCtx.DaemonSetName)
		require.NoError(t, err)

		// Wait for healthy pod to be running
		require.Eventually(t, func() bool {
			pods, err := listDaemonSetPods(ctx, client, testCtx.Namespace, testCtx.DaemonSetName)
			if err != nil || len(pods) == 0 {
				return false
			}
			if pods[0].Status.Phase == v1.PodRunning {
				t.Logf("First DaemonSet pod %s is now Running", pods[0].Name)
				return true
			}
			return false
		}, helpers.EventuallyWaitTimeout, helpers.WaitInterval)

		// Wait for first DaemonSet to be removed from annotation
		require.Eventually(t, func() bool {
			has1 := checkDaemonSetInAnnotation(ctx, t, client, testCtx.NodeName, testCtx.DaemonSetName)
			has2 := checkDaemonSetInAnnotation(ctx, t, client, testCtx.NodeName, testCtx.DaemonSetName2)
			t.Logf("After DS1 recovery: DS1=%v, DS2=%v", has1, has2)
			return !has1 && has2
		}, helpers.EventuallyWaitTimeout, helpers.WaitInterval)

		// CRITICAL: Node should STILL be cordoned because DS2 is still unhealthy
		time.Sleep(5 * time.Second)
		helpers.AssertQuarantineState(ctx, t, client, testCtx.NodeName, helpers.QuarantineAssertion{
			ExpectCordoned:   true,
			ExpectAnnotation: true,
		})
		t.Log("Node correctly remains cordoned - second DaemonSet is still unhealthy")

		return ctx
	})

	feature.Assess("Second DaemonSet recovery uncordons node", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		// Update second DaemonSet to healthy
		t.Logf("Updating second DaemonSet %s to healthy", testCtx.DaemonSetName2)
		err = updateDaemonSetToHealthy(ctx, client, testCtx.Namespace, testCtx.DaemonSetName2)
		require.NoError(t, err)

		// Wait for healthy pod to be running
		require.Eventually(t, func() bool {
			pods, err := listDaemonSetPods(ctx, client, testCtx.Namespace, testCtx.DaemonSetName2)
			if err != nil || len(pods) == 0 {
				return false
			}
			if pods[0].Status.Phase == v1.PodRunning {
				t.Logf("Second DaemonSet pod %s is now Running", pods[0].Name)
				return true
			}
			return false
		}, helpers.EventuallyWaitTimeout, helpers.WaitInterval)

		// Wait for annotation to be cleared
		require.Eventually(t, func() bool {
			has1 := checkDaemonSetInAnnotation(ctx, t, client, testCtx.NodeName, testCtx.DaemonSetName)
			has2 := checkDaemonSetInAnnotation(ctx, t, client, testCtx.NodeName, testCtx.DaemonSetName2)
			t.Logf("After DS2 recovery: DS1=%v, DS2=%v", has1, has2)
			return !has1 && !has2
		}, helpers.EventuallyWaitTimeout, helpers.WaitInterval)

		// Verify node is now uncordoned
		t.Log("Waiting for node to be uncordoned")
		helpers.WaitForNodesCordonState(ctx, t, client, []string{testCtx.NodeName}, false)
		t.Log("Node correctly uncordoned after ALL DaemonSet failures resolved")

		return ctx
	})

	feature.Assess("DaemonSet deletion uncordons node immediately", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		// Create a new failing DaemonSet for deletion test
		dsName := "test-ds-deletion"
		ds := createTestDaemonSetWithUniqueSelector(dsName, testCtx.Namespace, testCtx.NodeName, true)
		err = client.Resources().Create(ctx, ds)
		require.NoError(t, err)
		t.Logf("Created DaemonSet %s for deletion test", dsName)

		// Wait for pod to be created and in pending state
		require.Eventually(t, func() bool {
			pods, err := listDaemonSetPods(ctx, client, testCtx.Namespace, dsName)
			return err == nil && len(pods) > 0
		}, helpers.EventuallyWaitTimeout, helpers.WaitInterval)

		// Wait for policy timeout (need to wait again for this new DaemonSet)
		t.Log("Waiting for policy timeout for deletion test")
		time.Sleep(policyTimeoutWait)

		// Verify node is cordoned
		require.Eventually(t, func() bool {
			return checkDaemonSetInAnnotation(ctx, t, client, testCtx.NodeName, dsName)
		}, helpers.EventuallyWaitTimeout, helpers.WaitInterval)

		helpers.AssertQuarantineState(ctx, t, client, testCtx.NodeName, helpers.QuarantineAssertion{
			ExpectCordoned:   true,
			ExpectAnnotation: true,
		})

		// Delete the DaemonSet
		t.Log("Deleting DaemonSet to test immediate uncordon")
		cleanupDaemonSet(ctx, t, client, testCtx.Namespace, dsName)

		// Verify node is uncordoned after DaemonSet deletion
		t.Log("Waiting for node to be uncordoned after DaemonSet deletion")
		helpers.WaitForNodesCordonState(ctx, t, client, []string{testCtx.NodeName}, false)
		t.Log("Node correctly uncordoned after DaemonSet was deleted")

		return ctx
	})

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		// Clean up all DaemonSets
		t.Log("Cleaning up test DaemonSets")
		cleanupDaemonSet(ctx, t, client, testCtx.Namespace, testCtx.DaemonSetName)
		cleanupDaemonSet(ctx, t, client, testCtx.Namespace, testCtx.DaemonSetName2)
		cleanupDaemonSet(ctx, t, client, testCtx.Namespace, "test-ds-deletion")

		// Ensure node is uncordoned
		node, err := helpers.GetNodeByName(ctx, client, testCtx.NodeName)
		if err != nil {
			t.Logf("Warning: failed to get node for cleanup: %v", err)
		} else if node.Spec.Unschedulable {
			node.Spec.Unschedulable = false
			if updateErr := client.Resources().Update(ctx, node); updateErr != nil {
				t.Logf("Warning: failed to uncordon node during teardown: %v", updateErr)
			}
		}

		return ctx
	})

	testEnv.Test(t, feature.Feature())
}

// checkDaemonSetInAnnotation checks if a DaemonSet is tracked in the node annotation
func checkDaemonSetInAnnotation(ctx context.Context, t *testing.T, client klient.Client, nodeName, dsName string) bool {
	node, err := helpers.GetNodeByName(ctx, client, nodeName)
	if err != nil {
		return false
	}

	annotation, exists := node.Annotations[annotationKey]
	if !exists || annotation == "" || annotation == "{}" {
		return false
	}

	var annotationMap map[string]string
	if err := json.Unmarshal([]byte(annotation), &annotationMap); err != nil {
		return false
	}

	// Look for owner-level tracking key containing the DaemonSet name
	for key := range annotationMap {
		if strings.Contains(key, "DaemonSet/"+dsName+"/") {
			return true
		}
	}
	return false
}

// createTestDaemonSetWithUniqueSelector creates a DaemonSet with a unique selector
// This is needed when running multiple DaemonSets in the same namespace.
//
// When shouldFail=true, the pod uses an init container that exits with failure,
// causing the pod to stay in CrashLoopBackOff. This matches the policy condition
// which checks for pods NOT in Running/Succeeded phase and triggers after 2 minutes.
func createTestDaemonSetWithUniqueSelector(name, namespace, nodeName string, shouldFail bool) *appsv1.DaemonSet {
	// Use the DaemonSet name as part of the selector to make it unique
	selectorLabel := "app-" + name

	podSpec := v1.PodSpec{
		NodeSelector: map[string]string{
			"kubernetes.io/hostname": nodeName,
		},
		Containers: []v1.Container{
			{
				Name:    "main",
				Image:   "busybox:latest",
				Command: []string{"sh", "-c", "sleep 3600"},
			},
		},
		RestartPolicy: v1.RestartPolicyAlways,
		Tolerations: []v1.Toleration{
			{Operator: v1.TolerationOpExists},
		},
	}

	// For failing pods, add an init container that never completes
	// This keeps the pod in Pending (Init:0/1) state, which triggers the policy
	// (policy checks resource.status.phase != 'Running')
	if shouldFail {
		podSpec.InitContainers = []v1.Container{
			{
				Name:    "init-blocker",
				Image:   "busybox:latest",
				Command: []string{"sh", "-c", "sleep 3600"},
			},
		}
	}

	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app":  selectorLabel,
				"test": "kubernetes-object-monitor",
			},
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": selectorLabel,
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":  selectorLabel,
						"test": "kubernetes-object-monitor",
					},
				},
				Spec: podSpec,
			},
		},
	}
}

// Helper functions for DaemonSet tests
// listDaemonSetPods returns all pods owned by the specified DaemonSet
func listDaemonSetPods(ctx context.Context, client klient.Client, namespace, dsName string) ([]v1.Pod, error) {
	var podList v1.PodList
	err := client.Resources(namespace).List(ctx, &podList)
	if err != nil {
		return nil, err
	}

	var dsPods []v1.Pod
	for _, pod := range podList.Items {
		for _, ownerRef := range pod.OwnerReferences {
			if ownerRef.Kind == "DaemonSet" && ownerRef.Name == dsName {
				dsPods = append(dsPods, pod)
				break
			}
		}
	}
	return dsPods, nil
}

// cleanupDaemonSet deletes a DaemonSet and waits for it and its pods to be fully deleted
func cleanupDaemonSet(ctx context.Context, t *testing.T, client klient.Client, namespace, name string) {
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
	if err := client.Resources().Delete(ctx, ds); err != nil {
		t.Logf("Note: DaemonSet deletion returned error (may be expected if not found): %v", err)
	}

	// Wait for pods to be deleted
	require.Eventually(t, func() bool {
		pods, err := listDaemonSetPods(ctx, client, namespace, name)
		return err == nil && len(pods) == 0
	}, 2*time.Minute, 5*time.Second)

	// Wait for DaemonSet to be fully deleted
	require.Eventually(t, func() bool {
		err := client.Resources(namespace).Get(ctx, name, namespace, &appsv1.DaemonSet{})
		if err == nil {
			return false // Object still exists
		}
		if apierrors.IsNotFound(err) {
			return true // Deleted successfully
		}
		// Transient error - log and retry
		t.Logf("Note: transient error checking DaemonSet deletion: %v", err)
		return false
	}, 30*time.Second, 1*time.Second)
}

// updateDaemonSetToHealthy updates a DaemonSet to remove the blocking init container,
// allowing the pod to reach Running state.
func updateDaemonSetToHealthy(ctx context.Context, client klient.Client, namespace, name string) error {
	ds := &appsv1.DaemonSet{}
	err := client.Resources(namespace).Get(ctx, name, namespace, ds)
	if err != nil {
		return err
	}

	// Remove init containers to allow pod to start normally
	ds.Spec.Template.Spec.InitContainers = nil

	return client.Resources().Update(ctx, ds)
}
