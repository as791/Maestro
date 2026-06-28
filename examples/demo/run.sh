#!/usr/bin/env bash
# End-to-end demo: drive every Maestro workflow transition against a live kind
# cluster running the Flink Operator 1.15 + Flink 2.2, asserting real
# FlinkDeployment state after each step.
#
# Prereqs: the cluster is up (deploy/local 'make up'), plus kubectl, curl, jq.
set -euo pipefail

API="${MAESTRO_API:-http://localhost:8080}"
REGISTRY="${REGISTRY:-localhost:5001}"
ENV="${MAESTRO_ENV:-integration}"
NS="${MAESTRO_NS:-streaming}"
NAME="${MAESTRO_NAME:-orders}"
BASE="${API}/api/v1/deployments/${ENV}/${NS}/${NAME}"
CLUSTER_BASE="${API}/api/v1/clusters/${ENV}/${NS}"

say()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m   ✓ %s\033[0m\n' "$*"; }
fail() { printf '\033[1;31m   ✗ %s\033[0m\n' "$*"; exit 1; }

idem() { echo "$1-$(date +%s)-$RANDOM"; }

actor_field() { curl -fsS "${BASE}/actor" | jq -r "$1"; }

wait_for() { # <jq-filter> <expected> <description>
  local filter="$1" expected="$2" desc="$3" tries=60
  for _ in $(seq 1 $tries); do
    if [ "$(actor_field "$filter")" = "$expected" ]; then ok "$desc"; return 0; fi
    sleep 5
  done
  fail "timed out waiting for: $desc (got '$(actor_field "$filter")')"
}

show_flinkdeployment() {
  kubectl -n "$NS" get flinkdeployment "$NAME" \
    -o jsonpath='{.status.jobManagerDeploymentStatus} / {.status.jobStatus.state}{"\n"}' 2>/dev/null || true
}

wait_for_flink_job() { # <expected-state> <description>
  local expected="$1" desc="$2" tries=90 state=""
  for _ in $(seq 1 $tries); do
    state="$(kubectl -n "$NS" get flinkdeployment "$NAME" \
      -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || true)"
    if [ "$state" = "$expected" ]; then ok "$desc"; return 0; fi
    sleep 5
  done
  fail "timed out waiting for Flink job: $desc (got '${state:-missing}')"
}

# Resolve the digest of the freshly-pushed good job image.
say "Resolving job image digest"
GOOD_DIGEST="$(docker inspect --format='{{index .RepoDigests 0}}' ${REGISTRY}/wiki-edit-count:latest 2>/dev/null || true)"
[ -n "$GOOD_DIGEST" ] || fail "could not resolve digest for ${REGISTRY}/wiki-edit-count:latest (build/push it first)"
ok "image: $GOOD_DIGEST"
BAD_DIGEST="${REGISTRY}/wiki-edit-count@sha256:$(printf 'deadbeef%.0s' {1..8})"

good_spec() { # <parallelism>
  cat <<JSON
{
  "imageDigest": "${GOOD_DIGEST}",
  "flinkVersion": "2.2",
  "jobArgs": {
    "maestro.entryClass": "com.example.maestro.WikiEditCount",
    "bootstrap.servers": "maestro-kafka-bootstrap.kafka.svc:9092",
    "source.topic": "wikimedia.recentchange"
  },
  "flinkConfig": { "taskmanager.numberOfTaskSlots": "2" },
  "parallelism": ${1:-2},
  "maxParallelism": 128,
  "resources": { "taskManagerCpu": 1, "taskManagerMemoryMiB": 2048, "taskManagerCount": 1, "slotsPerManager": 2 },
  "stateCompatibility": { "jobGraphCompatible": true, "operatorUidsStable": true }
}
JSON
}

# 1. Register the deployment actor.
say "1/11 Register deployment actor"
curl -fsS -X PUT "${BASE}" -H 'Content-Type: application/json' \
  -d '{"owner":"streaming","serviceAccount":"flink","nodePool":"default"}' >/dev/null
ok "registered ${ENV}/${NS}/${NAME}"

# 2. Deploy v1.
say "2/11 Deploy initial version"
curl -fsS -X POST "${BASE}/deploy" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(idem deploy)" \
  -d "{\"requester\":\"demo\",\"approved\":true,\"spec\":$(good_spec 2)}" >/dev/null
wait_for '.currentVersion.versionId' '1' "version 1 is current and healthy"
wait_for_flink_job 'RUNNING' "Flink job is actually running"
show_flinkdeployment

# 3. Savepoint.
say "3/11 Trigger savepoint"
curl -fsS -X POST "${BASE}/savepoint" -H "Idempotency-Key: $(idem sp)" >/dev/null
savepoint_recorded=false
for _ in $(seq 1 60); do
  [ "$(actor_field '.lastSavepoint != null')" = "true" ] && { ok "savepoint recorded: $(actor_field '.lastSavepoint.uri')"; savepoint_recorded=true; break; }
  sleep 5
done
[ "$savepoint_recorded" = "true" ] || fail "timed out waiting for savepoint"

# 4. Scale (parallelism change -> new version via rollout).
say "4/11 Scale parallelism to 4"
curl -fsS -X POST "${BASE}/scale" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(idem scale)" -d '{"requester":"demo","parallelism":4,"approved":true}' >/dev/null
wait_for '.currentVersion.spec.parallelism' '4' "scaled to parallelism 4"
wait_for_flink_job 'RUNNING' "scaled Flink job is running"

# 5. Suspend.
say "5/11 Suspend"
curl -fsS -X POST "${BASE}/suspend" -H "Idempotency-Key: $(idem suspend)" >/dev/null
wait_for '.status' 'SUSPENDED' "actor suspended"

# 6. Resume.
say "6/11 Resume"
curl -fsS -X POST "${BASE}/resume" -H "Idempotency-Key: $(idem resume)" >/dev/null
wait_for '.status' 'IDLE' "actor resumed"
wait_for_flink_job 'RUNNING' "resumed Flink job is running"

# 7. Autoscaler enable + freeze.
say "7/11 Autoscaler enable + freeze"
curl -fsS -X POST "${BASE}/autoscaler/enable" -H "Idempotency-Key: $(idem as-on)" >/dev/null
wait_for '.autoscalerEnabled' 'true' "autoscaler enabled"
curl -fsS -X POST "${BASE}/autoscaler/freeze" -H "Idempotency-Key: $(idem as-frz)" >/dev/null
wait_for '.autoscalerFrozen' 'true' "autoscaler frozen"

# 8. Deploy a broken image -> health gate fails -> automatic rollback.
say "8/11 Deploy broken image (expect health-gate rollback)"
BROKEN_SPEC="$(good_spec 4 | jq --arg d "$BAD_DIGEST" '.imageDigest=$d')"
curl -fsS -X POST "${BASE}/deploy" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(idem bad)" \
  -d "{\"requester\":\"demo\",\"approved\":true,\"spec\":${BROKEN_SPEC}}" >/dev/null
for _ in $(seq 1 90); do
  last_status="$(actor_field '.recentOperations[-1].status')"
  [ "$last_status" = "FAILED" ] && { ok "rollout failed and previous version restored"; break; }
  sleep 5
done
[ "$(actor_field '.currentVersion.spec.imageDigest')" = "$GOOD_DIGEST" ] \
  && ok "current version is still the healthy image" \
  || fail "expected rollback to the healthy image"

# 9. Cluster freeze blocks mutations.
say "9/11 Cluster freeze blocks mutations"
curl -fsS -X POST "${CLUSTER_BASE}/freeze" -H 'Content-Type: application/json' \
  -d '{"requester":"incident-commander","reason":"demo"}' >/dev/null
code="$(curl -s -o /dev/null -w '%{http_code}' -X POST "${BASE}/scale" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $(idem frozen)" \
  -d '{"requester":"demo","parallelism":2,"approved":true}')"
[ "$code" = "409" ] && ok "mutation correctly rejected while frozen (HTTP 409)" || fail "expected 409, got $code"

# 10. Unfreeze.
say "10/11 Unfreeze"
curl -fsS -X POST "${CLUSTER_BASE}/unfreeze" -H 'Content-Type: application/json' \
  -d '{"requester":"incident-commander"}' >/dev/null
ok "cluster unfrozen"

# 11. Manual continue-as-new (actor state compaction).
say "11/11 Continue-as-new"
curl -fsS -X POST "${BASE}/continue-as-new" -H "Idempotency-Key: $(idem can)" >/dev/null
ok "continue-as-new signalled"

say "Demo complete — every transition exercised against a live Flink job."
kubectl -n "$NS" get flinkdeployment "$NAME" || true
