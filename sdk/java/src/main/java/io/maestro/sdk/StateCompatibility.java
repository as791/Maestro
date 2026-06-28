package io.maestro.sdk;

/** State compatibility flags for deployment upgrades. */
public record StateCompatibility(
        boolean jobGraphCompatible,
        boolean operatorUidsStable,
        boolean allowNonRestored,
        boolean freshStartApproved
) {
    /** Safe defaults: compatible graph, stable UIDs, no non-restored, no fresh start. */
    public static StateCompatibility safe() {
        return new StateCompatibility(true, true, false, false);
    }

    String toJson() {
        return """
                {"jobGraphCompatible":%b,"operatorUidsStable":%b,"allowNonRestored":%b,"freshStartApproved":%b}"""
                .formatted(jobGraphCompatible, operatorUidsStable, allowNonRestored, freshStartApproved);
    }
}
