#!/bin/bash

set -eu

docker compose -f infra/kafka.yaml up --build > /dev/null & 

go mod download
go run src/main.go

# docker compose -f infra/kafka.yaml down > /dev/null
