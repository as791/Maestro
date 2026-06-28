package maestro

import (
	"context"
	"log/slog"
	"time"
)

type AutoscalerBase interface {
	Evaluate(ctx context.Context, view *DeploymentActorView) (*int, error)
}

func RunAutoscaler(ctx context.Context, d *Deployment, evaluator AutoscalerBase, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			view, err := d.Status(ctx)
			if err != nil {
				slog.Error("autoscaler: status poll failed", "err", err)
				continue
			}
			if view.AutoscalerFrozen || !view.AutoscalerEnabled {
				continue
			}
			target, err := evaluator.Evaluate(ctx, view)
			if err != nil {
				slog.Error("autoscaler: evaluate failed", "err", err)
				continue
			}
			if target == nil {
				continue
			}
			current := 0
			if view.CurrentVersion != nil {
				current = view.CurrentVersion.Spec.Parallelism
			}
			if *target == current {
				continue
			}
			_, err = d.Scale(ctx, ScaleRequest{
				Requester:   "autoscaler",
				Parallelism: *target,
				Reason:      "autoscaler decision",
			})
			if err != nil {
				slog.Error("autoscaler: scale failed", "err", err, "target", *target)
			}
		}
	}
}
