package io.maestro.sdk;

import java.util.LinkedHashMap;
import java.util.Map;

/** Immutable deployment specification, built via {@link #builder()}. */
public record DeploymentSpec(
        String imageDigest,
        String flinkVersion,
        int parallelism,
        int maxParallelism,
        ResourceShape resources,
        StateCompatibility stateCompatibility,
        Map<String, String> jobArgs,
        Map<String, String> flinkConfig,
        boolean autoscalerEnabled
) {
    public static Builder builder() { return new Builder(); }

    String toJson() {
        var sb = new StringBuilder(512);
        sb.append('{');
        Json.field(sb, "imageDigest", imageDigest);
        Json.field(sb, "flinkVersion", flinkVersion);
        Json.field(sb, "parallelism", parallelism);
        Json.field(sb, "maxParallelism", maxParallelism);
        sb.append("\"resources\":").append(resources.toJson()).append(',');
        sb.append("\"stateCompatibility\":").append(stateCompatibility.toJson()).append(',');
        Json.mapField(sb, "jobArgs", jobArgs);
        Json.mapField(sb, "flinkConfig", flinkConfig);
        Json.field(sb, "autoscalerEnabled", autoscalerEnabled);
        // remove trailing comma
        if (sb.charAt(sb.length() - 1) == ',') sb.setLength(sb.length() - 1);
        sb.append('}');
        return sb.toString();
    }

    public static final class Builder {
        private String imageDigest;
        private String flinkVersion;
        private int parallelism = 1;
        private int maxParallelism = 128;
        private ResourceShape resources;
        private StateCompatibility stateCompatibility = StateCompatibility.safe();
        private final Map<String, String> jobArgs = new LinkedHashMap<>();
        private final Map<String, String> flinkConfig = new LinkedHashMap<>();
        private boolean autoscalerEnabled;

        private Builder() {}

        public Builder imageDigest(String v) { imageDigest = v; return this; }
        public Builder flinkVersion(String v) { flinkVersion = v; return this; }
        public Builder parallelism(int v) { parallelism = v; return this; }
        public Builder maxParallelism(int v) { maxParallelism = v; return this; }
        public Builder resources(ResourceShape v) { resources = v; return this; }
        public Builder stateCompatibility(StateCompatibility v) { stateCompatibility = v; return this; }
        public Builder jobArg(String k, String v) { jobArgs.put(k, v); return this; }
        public Builder jobArgs(Map<String, String> m) { jobArgs.putAll(m); return this; }
        public Builder flinkConfig(String k, String v) { flinkConfig.put(k, v); return this; }
        public Builder flinkConfig(Map<String, String> m) { flinkConfig.putAll(m); return this; }
        public Builder autoscalerEnabled(boolean v) { autoscalerEnabled = v; return this; }

        public DeploymentSpec build() {
            if (imageDigest == null || imageDigest.isBlank()) throw new IllegalArgumentException("imageDigest required");
            if (flinkVersion == null || flinkVersion.isBlank()) throw new IllegalArgumentException("flinkVersion required");
            if (resources == null) throw new IllegalArgumentException("resources required");
            return new DeploymentSpec(imageDigest, flinkVersion, parallelism, maxParallelism,
                    resources, stateCompatibility, Map.copyOf(jobArgs), Map.copyOf(flinkConfig), autoscalerEnabled);
        }
    }
}
