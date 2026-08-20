package kubernetes

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type typedClient struct {
	api kubernetes.Interface
}

// New creates a fleet driver authenticated with the pod's ServiceAccount token.
func New(ctx context.Context, options Options) (*Driver, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster Kubernetes configuration: %w", err)
	}
	api, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes client: %w", err)
	}
	return newDriver(ctx, options, &typedClient{api: api})
}

func (c *typedClient) GetDeployment(ctx context.Context, namespace, name string) (*appsv1.Deployment, error) {
	return c.api.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
}

func (c *typedClient) GetScale(ctx context.Context, namespace, name string) (*autoscalingv1.Scale, error) {
	return c.api.AppsV1().Deployments(namespace).GetScale(ctx, name, metav1.GetOptions{})
}

func (c *typedClient) UpdateScale(ctx context.Context, namespace, name string, scale *autoscalingv1.Scale) (*autoscalingv1.Scale, error) {
	return c.api.AppsV1().Deployments(namespace).UpdateScale(ctx, name, scale, metav1.UpdateOptions{})
}

func (c *typedClient) ListPods(ctx context.Context, namespace, selector string) ([]corev1.Pod, error) {
	pods, err := c.api.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	return pods.Items, nil
}

func (c *typedClient) DeletePod(ctx context.Context, namespace, name string, uid types.UID) error {
	return c.api.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	})
}
