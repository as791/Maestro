#!/bin/bash
set -e
export GOPATH="/Users/aryaman.sinha/Documents/flink-actor-control-plane/gopath"
export GOCACHE="/Users/aryaman.sinha/Documents/flink-actor-control-plane/gocache"
export GOPROXY="https://proxy.golang.org,direct"

echo "Tidying root module..."
go mod tidy

echo "Tidying backends/kubernetes..."
cd backends/kubernetes
go mod tidy

echo "Tidying examples/wikimedia-producer..."
cd ../examples/wikimedia-producer
go mod tidy

echo "All modules tidied successfully."
