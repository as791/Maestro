# Build the Kubernetes-backed Maestro worker.
# Build context MUST be the repository root so both modules and go.work are present:
#   docker build -f backends/kubernetes/Dockerfile -t maestro-k8s-worker .
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
    -o /out/worker ./backends/kubernetes/cmd/worker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/worker /worker
USER nonroot:nonroot
ENTRYPOINT ["/worker"]
