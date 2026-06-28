package io.maestro.sdk;

/** Resource allocation for Flink TaskManagers. */
public record ResourceShape(
        double taskManagerCpu,
        long taskManagerMemoryMiB,
        int taskManagerCount,
        int slotsPerManager
) {
    public int totalSlots() {
        return taskManagerCount * slotsPerManager;
    }

    String toJson() {
        return """
                {"taskManagerCpu":%s,"taskManagerMemoryMiB":%d,"taskManagerCount":%d,"slotsPerManager":%d}"""
                .formatted(taskManagerCpu, taskManagerMemoryMiB, taskManagerCount, slotsPerManager);
    }
}
