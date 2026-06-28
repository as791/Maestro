package maestro

import (
	"context"
	"fmt"
	"time"
)

type Deployment struct {
	client    *Client
	Env       string
	Namespace string
	Name      string
}

func (d *Deployment) path(suffix string) string {
	return fmt.Sprintf("/api/v1/deployments/%s/%s/%s%s", d.Env, d.Namespace, d.Name, suffix)
}

func (d *Deployment) Register(ctx context.Context, req RegisterRequest) (*RegisterResponse, error) {
	var result RegisterResponse
	if err := d.client.putJSON(ctx, d.path(""), req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (d *Deployment) Status(ctx context.Context) (*DeploymentActorView, error) {
	var result DeploymentActorView
	if err := d.client.get(ctx, d.path("/actor"), &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (d *Deployment) Versions(ctx context.Context) ([]DeploymentVersion, error) {
	var result []DeploymentVersion
	if err := d.client.get(ctx, d.path("/versions"), &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (d *Deployment) Deploy(ctx context.Context, req DeployRequest) (*CommandResponse, error) {
	var result CommandResponse
	if err := d.client.postMutation(ctx, d.path("/deploy"), req, "", &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (d *Deployment) Scale(ctx context.Context, req ScaleRequest) (*CommandResponse, error) {
	var result CommandResponse
	if err := d.client.postMutation(ctx, d.path("/scale"), req, "", &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (d *Deployment) Savepoint(ctx context.Context, req SimpleRequest) (*CommandResponse, error) {
	var result CommandResponse
	if err := d.client.postMutation(ctx, d.path("/savepoint"), req, "", &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (d *Deployment) Suspend(ctx context.Context, req SimpleRequest) (*CommandResponse, error) {
	var result CommandResponse
	if err := d.client.postMutation(ctx, d.path("/suspend"), req, "", &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (d *Deployment) Resume(ctx context.Context, req SimpleRequest) (*CommandResponse, error) {
	var result CommandResponse
	if err := d.client.postMutation(ctx, d.path("/resume"), req, "", &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (d *Deployment) Rollback(ctx context.Context, req RollbackRequest) (*CommandResponse, error) {
	var result CommandResponse
	if err := d.client.postMutation(ctx, d.path("/rollback"), req, "", &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (d *Deployment) EnableAutoscaler(ctx context.Context, req SimpleRequest) (*CommandResponse, error) {
	var result CommandResponse
	if err := d.client.postMutation(ctx, d.path("/autoscaler/enable"), req, "", &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (d *Deployment) FreezeAutoscaler(ctx context.Context, req SimpleRequest) (*CommandResponse, error) {
	var result CommandResponse
	if err := d.client.postMutation(ctx, d.path("/autoscaler/freeze"), req, "", &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (d *Deployment) ContinueAsNew(ctx context.Context, req SimpleRequest) (*CommandResponse, error) {
	var result CommandResponse
	if err := d.client.postMutation(ctx, d.path("/continue-as-new"), req, "", &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (d *Deployment) WaitHealthy(ctx context.Context, timeout time.Duration) (*DeploymentActorView, error) {
	deadline := time.Now().Add(timeout)
	interval := 2 * time.Second
	for {
		view, err := d.Status(ctx)
		if err != nil {
			return nil, err
		}
		if view.CurrentVersion != nil && view.CurrentVersion.HealthSummary.Healthy {
			return view, nil
		}
		if time.Now().After(deadline) {
			return view, fmt.Errorf("maestro: deployment not healthy after %s (status=%s)", timeout, view.Status)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
}
