# Monitoring Critical Operators (pods in gpu-operator & network-operator namespace)

## Overview
For an NVIDIA GPU cluster to function correctly, critical infrastructure components under **gpu-operator** and **network-operator** namespace must be healthy. If these operators fail, the underlying hardware cannot be utilized effectively.

NVSentinel provides a built-in mechanism to monitor these operators and report health events when their pods are not running correctly.

## Configuration
To monitor the GPU and Network operators, you must enable the `kubernetes-object-monitor` component and define the monitoring policies in your NVSentinel `values.yaml`.

Add the following configuration to your `values.yaml`:

```yaml
# 1. Enable the component
global:
  kubernetesObjectMonitor:
    enabled: true

# 2. Configure the policies
kubernetes-object-monitor:
  policies:
    # Policy 1: Monitor GPU Operator Namespace
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

    # Policy 2: Monitor Network Operator Namespace
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
If you receive these events, you can start with the below investigatory action:
1.  Check the logs of the failing pods in the respective namespace:
    ```bash
    kubectl get pods -n gpu-operator -o wide
    kubectl logs -n gpu-operator <pod-name>
    ```
2.  Verify if the node itself is Ready.
3.  Check for image pull errors or configuration issues in the Operator deployment.
