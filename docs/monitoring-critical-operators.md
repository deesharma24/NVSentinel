# Monitoring Critical Operators (pods in gpu-operator & network-operator namespace)

## Overview
For an NVIDIA GPU cluster to function correctly, critical infrastructure components under **gpu-operator** and **network-operator** namespace must be healthy. If these operators fail, the underlying hardware cannot be utilized effectively.

NVSentinel provides a built-in mechanism to monitor these operators and report health events when their pods are not running correctly.

## Prerequisites
*   **Kubernetes Object Monitor**: Ensure the `kubernetes-object-monitor` component is enabled in your NVSentinel installation.

## Configuration
To monitor for pods in gpu-operator and network-operator namespace, add the following policies to your NVSentinel Helm values or ConfigMap.

These policies monitor all pods in the `gpu-operator` and `network-operator` namespaces. If a pod is assigned to a node but fails to reach the `Running` state (and is not `Succeeded`), a health event is generated for that specific node.

### GPU Operator Policy
This policy triggers a `Software` error if any pod in the `gpu-operator` namespace is unhealthy.

```yaml
- name: gpu-operator-pods-health
  enabled: true
  resource:
    group: ""
    version: v1
    kind: Pod
  predicate:
    # Trigger event if:
    # 1. Pod is in gpu-operator namespace
    # 2. Pod has been assigned to a node (nodeName is set)
    # 3. Pod is NOT Running AND NOT Succeeded
    expression: |
      resource.metadata.namespace == 'gpu-operator' && 
      has(resource.spec.nodeName) && resource.spec.nodeName != "" &&
      resource.status.phase != 'Running' && 
      resource.status.phase != 'Succeeded'
  nodeAssociation:
    expression: resource.spec.nodeName
  healthEvent:
    componentClass: Software
    isFatal: true
    message: "GPU Operator pod is not in Running state"
    recommendedAction: CONTACT_SUPPORT
    errorCode:
      - GPU_OPERATOR_UNHEALTHY
```

### Network Operator Policy
This policy triggers a `Software` error if any pod in the `network-operator` namespace is unhealthy.

```yaml
- name: network-operator-pods-health
  enabled: true
  resource:
    group: ""
    version: v1
    kind: Pod
  predicate:
    expression: |
      resource.metadata.namespace == 'network-operator' && 
      has(resource.spec.nodeName) && resource.spec.nodeName != "" &&
      resource.status.phase != 'Running' && 
      resource.status.phase != 'Succeeded'
  nodeAssociation:
    expression: resource.spec.nodeName
  healthEvent:
    componentClass: Software
    isFatal: true
    message: "Network Operator pod is not in Running state"
    recommendedAction: CONTACT_SUPPORT
    errorCode:
      - NETWORK_OPERATOR_UNHEALTHY
```

## Troubleshooting
If you receive these alerts:
1.  Check the logs of the failing pods in the respective namespace:
    ```bash
    kubectl get pods -n gpu-operator -o wide
    kubectl logs -n gpu-operator <pod-name>
    ```
2.  Verify if the node itself is Ready.
3.  Check for image pull errors or configuration issues in the Operator deployment.
