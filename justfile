#!/usr/bin/env just --justfile
GO := "go"

GOVET_COMMAND := GO + " vet"
GOTEST_COMMAND := GO + " test"
GOCOVER_COMMAND := GO + " tool cover"
GOBUILD_COMMAND := GO + " build"
update:
  go get -u
  go mod tidy -v

# Clean dist directory and rebuild the binary file
build-owlpost:
  rm -rf ./dist && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o ./dist/app ./cmd/owlpost
