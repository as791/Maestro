package maestro

import (
	"fmt"
	"hash/fnv"
)

// Task-queue sharding spreads actor-workflow task matching across several
// Temporal task queues. A single task queue is fine for hundreds of actors;
// once a deployment fleet reaches thousands of always-open actor workflows,
// fanning them across shards keeps matching throughput and per-queue backlog
// healthy. See docs/SCALING.md.
//
// Sharding only affects where an actor workflow is *started*; signals and
// queries are routed by workflow ID and are unaffected, so the scheme is purely
// additive and safe to enable on an existing deployment for newly started
// actors.

// ActorTaskQueues returns every actor task queue a worker fleet must poll for a
// given base name and shard count. With shards <= 1 it returns just the base.
func ActorTaskQueues(base string, shards int) []string {
	if shards <= 1 {
		return []string{base}
	}
	queues := make([]string, shards)
	for i := 0; i < shards; i++ {
		queues[i] = shardName(base, i)
	}
	return queues
}

// ShardTaskQueue returns the actor task queue that owns a given workflow ID.
func ShardTaskQueue(base string, shards int, workflowID string) string {
	if shards <= 1 {
		return base
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(workflowID))
	return shardName(base, int(h.Sum32()%uint32(shards)))
}

func shardName(base string, shard int) string {
	return fmt.Sprintf("%s-%d", base, shard)
}
