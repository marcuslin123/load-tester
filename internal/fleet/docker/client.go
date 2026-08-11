package docker

import (
	"context"
	"fmt"

	"github.com/containerd/errdefs"
	containertypes "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

type mobyClient struct {
	api *client.Client
}

// New validates the local worker image before returning a Docker fleet driver.
func New(ctx context.Context, options Options) (*Driver, error) {
	api, err := client.New(client.FromEnv, client.WithUserAgent("load-tester"))
	if err != nil {
		return nil, fmt.Errorf("create Docker client: %w", err)
	}
	driver, err := newDriver(ctx, options, &mobyClient{api: api})
	if err != nil {
		_ = api.Close()
		return nil, err
	}
	return driver, nil
}

func (m *mobyClient) ImageExists(ctx context.Context, image string) (bool, error) {
	_, err := m.api.ImageInspect(ctx, image)
	if errdefs.IsNotFound(err) {
		return false, nil
	}
	return err == nil, err
}

func (m *mobyClient) List(ctx context.Context, labels map[string]string) ([]engineContainer, error) {
	filters := client.Filters{}
	for key, value := range labels {
		filters = filters.Add("label", key+"="+value)
	}
	result, err := m.api.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: filters})
	if err != nil {
		return nil, err
	}
	containers := make([]engineContainer, 0, len(result.Items))
	for _, item := range result.Items {
		containers = append(containers, engineContainer{
			ID:      item.ID,
			Labels:  item.Labels,
			State:   string(item.State),
			Running: item.State == containertypes.StateRunning,
		})
	}
	return containers, nil
}

func (m *mobyClient) Create(ctx context.Context, spec containerSpec) (string, error) {
	result, err := m.api.ContainerCreate(ctx, toMobyCreateOptions(spec))
	if err != nil {
		return "", err
	}
	return result.ID, nil
}

func toMobyCreateOptions(spec containerSpec) client.ContainerCreateOptions {
	return client.ContainerCreateOptions{
		Name: spec.Name,
		Config: &containertypes.Config{
			Image:  spec.Image,
			Env:    spec.Env,
			Labels: spec.Labels,
		},
		HostConfig: &containertypes.HostConfig{
			NetworkMode:   containertypes.NetworkMode(spec.Network),
			ExtraHosts:    spec.ExtraHosts,
			RestartPolicy: containertypes.RestartPolicy{Name: containertypes.RestartPolicyMode(spec.RestartPolicy)},
		},
	}
}

func (m *mobyClient) Start(ctx context.Context, id string) error {
	_, err := m.api.ContainerStart(ctx, id, client.ContainerStartOptions{})
	return err
}

func (m *mobyClient) Inspect(ctx context.Context, id string) (engineContainer, error) {
	result, err := m.api.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		return engineContainer{}, err
	}
	state := result.Container.State
	if state == nil {
		return engineContainer{ID: result.Container.ID}, nil
	}
	return engineContainer{
		ID:      result.Container.ID,
		Labels:  result.Container.Config.Labels,
		State:   string(state.Status),
		Running: state.Running,
	}, nil
}

func (m *mobyClient) Remove(ctx context.Context, id string) error {
	_, err := m.api.ContainerRemove(ctx, id, client.ContainerRemoveOptions{Force: true, RemoveVolumes: true})
	return err
}

func (m *mobyClient) Close() error {
	return m.api.Close()
}
