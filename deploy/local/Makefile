# Local end-to-end environment for Maestro on kind:
#   Flink Kubernetes Operator 1.15 + Flink 2.2, Strimzi Kafka fed by the
#   Wikimedia EventStreams producer, and the Maestro control plane (real k8s backend).
#
# Usage:
#   make up      # bring everything up (idempotent)
#   make demo    # drive every workflow transition against the live cluster
#   make down    # tear everything down
#
# Prerequisites: docker, kind, kubectl, helm.

REPO_ROOT := $(shell git rev-parse --show-toplevel)
REGISTRY := localhost:5001
CLUSTER := maestro
FLINK_OPERATOR_VERSION ?= 1.15.0
CERT_MANAGER_VERSION ?= v1.16.2
STRIMZI_VERSION ?= 1.0.1

.PHONY: up down registry cluster cert-manager operator kafka images maestro producer wait-kafka demo

up: registry cluster cert-manager operator kafka images maestro producer
	@echo ">> Maestro local environment is ready. Run 'make demo'."

## --- infrastructure ---

registry:
	@if [ -z "$$(docker ps -q -f name=kind-registry)" ]; then \
		echo ">> starting local registry at $(REGISTRY)"; \
		docker run -d --restart=always -p 5001:5000 --name kind-registry registry:2; \
	fi

cluster:
	@if ! kind get clusters | grep -q '^$(CLUSTER)$$'; then \
		echo ">> creating kind cluster"; \
		kind create cluster --config kind-cluster.yaml; \
	fi
	@docker network connect kind kind-registry 2>/dev/null || true
	@kubectl apply -f manifests/namespaces.yaml

cert-manager:
	@echo ">> installing cert-manager $(CERT_MANAGER_VERSION)"
	@kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/$(CERT_MANAGER_VERSION)/cert-manager.yaml
	@kubectl -n cert-manager rollout status deploy/cert-manager-webhook --timeout=180s

operator:
	@echo ">> installing Flink Kubernetes Operator $(FLINK_OPERATOR_VERSION)"
	@helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-$(FLINK_OPERATOR_VERSION)/ 2>/dev/null || true
	@helm repo update >/dev/null
	@helm upgrade --install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
		--namespace maestro-system \
		--set watchNamespaces="{streaming}"
	@kubectl -n maestro-system rollout status deploy/flink-kubernetes-operator --timeout=180s

kafka: wait-kafka

wait-kafka:
	@echo ">> installing Strimzi $(STRIMZI_VERSION) + Kafka"
	@curl -sL "https://github.com/strimzi/strimzi-kafka-operator/releases/download/$(STRIMZI_VERSION)/strimzi-cluster-operator-$(STRIMZI_VERSION).yaml" \
		| sed 's/namespace: myproject/namespace: kafka/' \
		| kubectl apply -n kafka -f -
	@kubectl -n kafka rollout status deploy/strimzi-cluster-operator --timeout=180s
	@kubectl wait --for condition=established --timeout=120s \
		crd/kafkas.kafka.strimzi.io crd/kafkanodepools.kafka.strimzi.io crd/kafkatopics.kafka.strimzi.io
	@kubectl apply -f manifests/kafka.yaml
	@echo ">> waiting for Kafka to become ready (this can take a few minutes)"
	@kubectl -n kafka wait kafka/maestro --for=condition=Ready --timeout=600s

## --- application images ---

images:
	@echo ">> building and pushing images to $(REGISTRY)"
	docker build -f $(REPO_ROOT)/Dockerfile --build-arg TARGET=control-api -t $(REGISTRY)/maestro-control-api:latest $(REPO_ROOT)
	docker build -f $(REPO_ROOT)/backends/kubernetes/Dockerfile -t $(REGISTRY)/maestro-k8s-worker:latest $(REPO_ROOT)
	docker build -t $(REGISTRY)/wikimedia-producer:latest $(REPO_ROOT)/examples/wikimedia-producer
	docker build -t $(REGISTRY)/wiki-edit-count:latest $(REPO_ROOT)/examples/flink-job
	docker push $(REGISTRY)/maestro-control-api:latest
	docker push $(REGISTRY)/maestro-k8s-worker:latest
	docker push $(REGISTRY)/wikimedia-producer:latest
	docker push $(REGISTRY)/wiki-edit-count:latest

## --- Maestro control plane (bundled Temporal for local use) ---

maestro:
	@echo ">> installing Maestro (helm, bundled Temporal)"
	@helm dependency build $(REPO_ROOT)/deploy/helm/fcp >/dev/null 2>&1 || true
	@helm upgrade --install maestro $(REPO_ROOT)/deploy/helm/fcp \
		--namespace maestro-system \
		--values $(REPO_ROOT)/deploy/helm/fcp/values-local.yaml
	@kubectl -n maestro-system rollout status deploy/maestro-control-api --timeout=240s

producer:
	@kubectl apply -f $(REPO_ROOT)/examples/wikimedia-producer/k8s/producer.yaml

## --- demo + teardown ---

demo:
	@$(REPO_ROOT)/examples/demo/run.sh

down:
	@kind delete cluster --name $(CLUSTER) || true
	@docker rm -f kind-registry || true
