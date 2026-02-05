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

package helpers

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
)

const (
	K8S_DEPLOYMENT_NAME = "kubernetes-object-monitor"
	K8S_CONTAINER_NAME  = "kubernetes-object-monitor"

	// K8sObjectMonitorAnnotationKey is the annotation key for policy matches set by kubernetes-object-monitor
	K8sObjectMonitorAnnotationKey = "nvsentinel.nvidia.com/k8s-object-monitor-policy-matches"
)

type KubernetesObjectMonitorTestContext struct {
	NodeName        string
	ConfigMapBackup []byte
	TestNamespace   string
}

func TeardownKubernetesObjectMonitor(
	ctx context.Context, t *testing.T, c *envconf.Config, configMapBackup []byte, originalArgs []string,
) {
	t.Helper()

	client, err := c.NewClient()
	require.NoError(t, err)

	if configMapBackup != nil {
		t.Log("Restoring configmap from memory")

		err = createConfigMapFromBytes(ctx, client, configMapBackup, "kubernetes-object-monitor", NVSentinelNamespace)
		require.NoError(t, err)

		err = RestartDeployment(ctx, t, client, K8S_DEPLOYMENT_NAME, NVSentinelNamespace)
		require.NoError(t, err)
	}

	err = RestoreDeploymentArgs(t, ctx, client, K8S_DEPLOYMENT_NAME, NVSentinelNamespace, K8S_CONTAINER_NAME, originalArgs)
	require.NoError(t, err)

	WaitForDeploymentRollout(ctx, t, client, K8S_DEPLOYMENT_NAME, NVSentinelNamespace)
}

func UpdateKubernetesObjectMonitorConfigMap(ctx context.Context, t *testing.T, client klient.Client,
	configMapPath string, configName string) {
	t.Helper()

	if configMapPath == "" {
		t.Fatalf("configMapPath is empty")
	}

	t.Logf("Updating configmap %s", configName)

	err := createConfigMapFromFilePath(ctx, client, configMapPath, configName, NVSentinelNamespace)
	require.NoError(t, err)

	t.Logf("Restarting %s deployment", K8S_DEPLOYMENT_NAME)

	err = RestartDeployment(ctx, t, client, K8S_DEPLOYMENT_NAME, NVSentinelNamespace)
	require.NoError(t, err)
}

// DaemonSetFailureType specifies the type of failure to simulate in a test DaemonSet.
type DaemonSetFailureType int

const (
	// DaemonSetHealthy creates a healthy DaemonSet with no failures
	DaemonSetHealthy DaemonSetFailureType = iota
	// DaemonSetInitBlocking creates a DaemonSet with init container that blocks (sleep)
	DaemonSetInitBlocking
	// DaemonSetInitCrashLoop creates a DaemonSet with init container that crashes (exit 1)
	DaemonSetInitCrashLoop
	// DaemonSetMainCrashLoop creates a DaemonSet with main container that crashes (exit 1)
	DaemonSetMainCrashLoop
)

// CreateTestDaemonSet creates a test DaemonSet with the specified failure type.
//
// Failure types and their effects:
//   - DaemonSetHealthy: Pod reaches Running state normally
//   - DaemonSetInitBlocking: Pod stuck in Pending/Init (phase=Pending, detected by phase check)
//   - DaemonSetInitCrashLoop: Init container CrashLoopBackOff (phase=Pending, detected by phase check)
//   - DaemonSetMainCrashLoop: Main container CrashLoopBackOff (phase=Running, detected by containerStatuses check)
func CreateTestDaemonSet(name, namespace, nodeName string, failureType DaemonSetFailureType) *appsv1.DaemonSet {
	selectorLabel := "app-" + name
	// Use short termination grace period for fast cleanup during tests
	terminationGracePeriod := int64(1)

	podSpec := v1.PodSpec{
		NodeSelector: map[string]string{
			"kubernetes.io/hostname": nodeName,
		},
		RestartPolicy:                 v1.RestartPolicyAlways,
		TerminationGracePeriodSeconds: &terminationGracePeriod,
		Tolerations: []v1.Toleration{
			{Operator: v1.TolerationOpExists},
		},
	}

	switch failureType {
	case DaemonSetMainCrashLoop:
		// Main container crashes immediately - pod phase=Running but container in CrashLoopBackOff
		podSpec.Containers = []v1.Container{
			{
				Name:    "crasher",
				Image:   "busybox:latest",
				Command: []string{"sh", "-c", "exit 1"},
			},
		}
	case DaemonSetInitCrashLoop:
		// Init container crashes immediately - pod phase=Pending with init in CrashLoopBackOff
		podSpec.InitContainers = []v1.Container{
			{
				Name:    "init-crasher",
				Image:   "busybox:latest",
				Command: []string{"sh", "-c", "exit 1"},
			},
		}
		podSpec.Containers = []v1.Container{
			{
				Name:    "main",
				Image:   "busybox:latest",
				Command: []string{"sh", "-c", "sleep 3600"},
			},
		}
	case DaemonSetInitBlocking:
		// Init container blocks forever - pod stuck in Pending/Init state
		podSpec.InitContainers = []v1.Container{
			{
				Name:    "init-blocker",
				Image:   "busybox:latest",
				Command: []string{"sh", "-c", "sleep 3600"},
			},
		}
		podSpec.Containers = []v1.Container{
			{
				Name:    "main",
				Image:   "busybox:latest",
				Command: []string{"sh", "-c", "sleep 3600"},
			},
		}
	case DaemonSetHealthy:
		// Healthy pod - main container runs normally
		podSpec.Containers = []v1.Container{
			{
				Name:    "main",
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

// CreateTestDaemonSetWithCrashLoop creates a DaemonSet with a main container that crashes.
// Shorthand for CreateTestDaemonSet(name, namespace, nodeName, DaemonSetMainCrashLoop).
func CreateTestDaemonSetWithCrashLoop(name, namespace, nodeName string) *appsv1.DaemonSet {
	return CreateTestDaemonSet(name, namespace, nodeName, DaemonSetMainCrashLoop)
}

// CreateTestDaemonSetWithInitCrashLoop creates a DaemonSet with an init container that crashes.
// Shorthand for CreateTestDaemonSet(name, namespace, nodeName, DaemonSetInitCrashLoop).
func CreateTestDaemonSetWithInitCrashLoop(name, namespace, nodeName string) *appsv1.DaemonSet {
	return CreateTestDaemonSet(name, namespace, nodeName, DaemonSetInitCrashLoop)
}

// CreateTestDaemonSetWithUniqueSelector creates a DaemonSet with optional init blocker.
// When shouldFail=true, uses DaemonSetInitBlocking; otherwise uses DaemonSetHealthy.
func CreateTestDaemonSetWithUniqueSelector(name, namespace, nodeName string, shouldFail bool) *appsv1.DaemonSet {
	if shouldFail {
		return CreateTestDaemonSet(name, namespace, nodeName, DaemonSetInitBlocking)
	}

	return CreateTestDaemonSet(name, namespace, nodeName, DaemonSetHealthy)
}

// ListDaemonSetPods returns all pods owned by the specified DaemonSet.
func ListDaemonSetPods(ctx context.Context, client klient.Client, namespace, dsName string) ([]v1.Pod, error) {
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

// CleanupDaemonSet deletes a DaemonSet and waits for it and its pods to be fully deleted.
func CleanupDaemonSet(ctx context.Context, t *testing.T, client klient.Client, namespace, name string) {
	t.Helper()

	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}

	// Check if DaemonSet exists first - skip cleanup if not found
	err := client.Resources(namespace).Get(ctx, name, namespace, ds)
	if apierrors.IsNotFound(err) {
		t.Logf("DaemonSet %s/%s does not exist, skipping cleanup", namespace, name)
		return
	}

	if err != nil {
		t.Logf("Warning: error checking DaemonSet existence: %v", err)
	}

	// Delete the DaemonSet
	if err := client.Resources().Delete(ctx, ds); err != nil {
		if apierrors.IsNotFound(err) {
			t.Logf("DaemonSet %s/%s already deleted", namespace, name)
			return
		}

		t.Logf("Note: DaemonSet deletion returned error: %v", err)
	}

	// Wait for pods to be deleted (short timeout since terminationGracePeriodSeconds=1)
	require.Eventually(t, func() bool {
		pods, err := ListDaemonSetPods(ctx, client, namespace, name)
		return err == nil && len(pods) == 0
	}, 30*time.Second, 1*time.Second, "timed out waiting for pods of DaemonSet %s/%s to be deleted", namespace, name)

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
	}, 15*time.Second, 1*time.Second, "timed out waiting for DaemonSet %s/%s to be fully deleted", namespace, name)
}

// updateDaemonSet is a helper that gets a DaemonSet, applies a modifier function,
// and updates it.
func updateDaemonSet(
	ctx context.Context, client klient.Client, namespace, name string, modifier func(*appsv1.DaemonSet),
) error {
	ds := &appsv1.DaemonSet{}

	err := client.Resources(namespace).Get(ctx, name, namespace, ds)
	if err != nil {
		return err
	}

	modifier(ds)

	return client.Resources().Update(ctx, ds)
}

// UpdateDaemonSetToHealthy updates a DaemonSet to remove the blocking init container,
// allowing the pod to reach Running state.
func UpdateDaemonSetToHealthy(ctx context.Context, client klient.Client, namespace, name string) error {
	return updateDaemonSet(ctx, client, namespace, name, func(ds *appsv1.DaemonSet) {
		ds.Spec.Template.Spec.InitContainers = nil
	})
}

// FixCrashingContainer updates a DaemonSet to fix the crashing main container.
// Changes the container command from "exit 1" to "sleep 3600" so the pod becomes healthy.
func FixCrashingContainer(ctx context.Context, client klient.Client, namespace, name string) error {
	return updateDaemonSet(ctx, client, namespace, name, func(ds *appsv1.DaemonSet) {
		for i := range ds.Spec.Template.Spec.Containers {
			if ds.Spec.Template.Spec.Containers[i].Name == "crasher" {
				ds.Spec.Template.Spec.Containers[i].Command = []string{"sh", "-c", "sleep 3600"}
			}
		}
	})
}

// FixCrashingInitContainer updates a DaemonSet to fix the crashing init container.
// Changes the init container command from "exit 1" to "exit 0" so the pod can start.
func FixCrashingInitContainer(ctx context.Context, client klient.Client, namespace, name string) error {
	return updateDaemonSet(ctx, client, namespace, name, func(ds *appsv1.DaemonSet) {
		for i := range ds.Spec.Template.Spec.InitContainers {
			if ds.Spec.Template.Spec.InitContainers[i].Name == "init-crasher" {
				ds.Spec.Template.Spec.InitContainers[i].Command = []string{"sh", "-c", "exit 0"}
			}
		}
	})
}

// checkPodCrashLoopBackOff checks if a pod has CrashLoopBackOff in its container statuses.
// Returns true if CrashLoopBackOff is found.
func checkPodCrashLoopBackOff(statuses []v1.ContainerStatus) bool {
	for _, cs := range statuses {
		if cs.State.Waiting != nil && cs.State.Waiting.Reason == "CrashLoopBackOff" {
			return true
		}
	}

	return false
}

// logContainerStatuses logs the current state of container statuses for debugging.
func logContainerStatuses(t *testing.T, statuses []v1.ContainerStatus, containerType string) {
	t.Helper()

	for _, cs := range statuses {
		switch {
		case cs.State.Waiting != nil:
			t.Logf("  %s %s: waiting (reason=%s)", containerType, cs.Name, cs.State.Waiting.Reason)
		case cs.State.Running != nil:
			t.Logf("  %s %s: running", containerType, cs.Name)
		case cs.State.Terminated != nil:
			t.Logf("  %s %s: terminated (exitCode=%d, reason=%s)",
				containerType, cs.Name, cs.State.Terminated.ExitCode, cs.State.Terminated.Reason)
		}
	}
}

// waitForCrashLoopBackOff is a helper that waits for CrashLoopBackOff in either
// containerStatuses or initContainerStatuses based on the checkInit flag.
func waitForCrashLoopBackOff(
	ctx context.Context, t *testing.T, client klient.Client,
	namespace, podNamePrefix string, checkInit bool,
) *v1.Pod {
	t.Helper()

	var foundPod *v1.Pod

	containerType := "Container"
	if checkInit {
		containerType = "InitContainer"
	}

	require.Eventually(t, func() bool {
		var podList v1.PodList
		if err := client.Resources(namespace).List(ctx, &podList); err != nil {
			return false
		}

		for i := range podList.Items {
			pod := &podList.Items[i]

			// Find pod by prefix (DaemonSet pods have generated suffixes)
			if !strings.HasPrefix(pod.Name, podNamePrefix) {
				continue
			}

			// Select which container statuses to check
			statuses := pod.Status.ContainerStatuses
			if checkInit {
				statuses = pod.Status.InitContainerStatuses
			}

			if checkPodCrashLoopBackOff(statuses) {
				t.Logf("Pod %s %s is in CrashLoopBackOff (phase: %s)",
					pod.Name, containerType, pod.Status.Phase)
				foundPod = pod

				return true
			}

			// Log current state for debugging
			t.Logf("Pod %s: phase=%s, %sStatuses=%d",
				pod.Name, pod.Status.Phase, containerType, len(statuses))
			logContainerStatuses(t, statuses, containerType)
		}

		return false
	}, EventuallyWaitTimeout, WaitInterval,
		"pod with prefix %s %s did not enter CrashLoopBackOff", podNamePrefix, containerType)

	return foundPod
}

// WaitForPodCrashLoopBackOff waits for a pod's main container to enter CrashLoopBackOff state.
// Returns the pod details once CrashLoopBackOff is detected in containerStatuses.
func WaitForPodCrashLoopBackOff(
	ctx context.Context, t *testing.T, client klient.Client, namespace, podNamePrefix string,
) *v1.Pod {
	return waitForCrashLoopBackOff(ctx, t, client, namespace, podNamePrefix, false)
}

// WaitForInitContainerCrashLoopBackOff waits for a pod's init container to enter
// CrashLoopBackOff state. Returns the pod details once CrashLoopBackOff is detected.
func WaitForInitContainerCrashLoopBackOff(
	ctx context.Context, t *testing.T, client klient.Client, namespace, podNamePrefix string,
) *v1.Pod {
	return waitForCrashLoopBackOff(ctx, t, client, namespace, podNamePrefix, true)
}

// WaitForQuarantineState polls until the node reaches the expected quarantine state or times out.
// This is more efficient than time.Sleep as it returns as soon as the condition is met.
func WaitForQuarantineState(
	ctx context.Context, t *testing.T, client klient.Client,
	nodeName string, expectCordoned bool, timeout time.Duration,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		node, err := GetNodeByName(ctx, client, nodeName)
		if err != nil {
			t.Logf("Error getting node: %v", err)
			return false
		}

		return node.Spec.Unschedulable == expectCordoned
	}, timeout, WaitInterval,
		"node %s did not reach expected cordon state %v within %v", nodeName, expectCordoned, timeout)
}

// WaitForDaemonSetPodRunning waits for a DaemonSet pod to reach Running state with all
// containers ready. This is useful after fixing a crashing container to verify recovery.
func WaitForDaemonSetPodRunning(
	ctx context.Context, t *testing.T, client klient.Client, namespace, dsName, nodeName string,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		pods, err := ListDaemonSetPods(ctx, client, namespace, dsName)
		if err != nil {
			t.Logf("Error listing pods: %v", err)
			return false
		}

		for _, pod := range pods {
			// Check if pod is on the expected node
			if pod.Spec.NodeName != nodeName {
				continue
			}

			// Check if pod is Running
			if pod.Status.Phase != v1.PodRunning {
				t.Logf("Pod %s phase: %s (waiting for Running)", pod.Name, pod.Status.Phase)
				return false
			}

			// Check if all containers are ready
			allReady := true

			for _, cs := range pod.Status.ContainerStatuses {
				if !cs.Ready {
					t.Logf("Pod %s container %s not ready yet", pod.Name, cs.Name)

					allReady = false
				}
			}

			if allReady && len(pod.Status.ContainerStatuses) > 0 {
				t.Logf("Pod %s is Running with all containers ready", pod.Name)
				return true
			}
		}

		return false
	}, EventuallyWaitTimeout, WaitInterval,
		"DaemonSet %s pod on node %s did not reach Running state", dsName, nodeName)
}
