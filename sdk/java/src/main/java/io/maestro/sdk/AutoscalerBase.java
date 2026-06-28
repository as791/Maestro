package io.maestro.sdk;

/**
 * Base for custom autoscaler implementations that interact with Maestro.
 * Subclass and implement {@link #evaluate} to build a custom scaling loop.
 */
public abstract class AutoscalerBase {
    protected final Deployment deployment;
    protected final String requester;

    protected AutoscalerBase(Deployment deployment, String requester) {
        this.deployment = deployment;
        this.requester = requester;
    }

    /**
     * Called by the scaling loop. Inspect current state and return desired parallelism,
     * or -1 to skip scaling this cycle.
     */
    protected abstract int evaluate(MaestroResponse actorState);

    /**
     * Run one evaluation cycle: fetch state, evaluate, scale if needed.
     * @return the MaestroResponse from the scale call, or null if no scaling was needed.
     */
    public MaestroResponse tick() {
        MaestroResponse state = deployment.actor();
        int desired = evaluate(state);
        if (desired <= 0) return null;

        var cv = state.getObject("currentVersion");
        if (cv != null) {
            var spec = cv.get("spec");
            if (spec instanceof java.util.Map<?,?> specMap) {
                Object current = specMap.get("parallelism");
                if (current instanceof Number n && n.intValue() == desired) return null;
            }
        }
        return deployment.scale(requester, desired, true, "autoscaler");
    }
}
