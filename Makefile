.PHONY: build test fmt lint clean show-coverage

VERSION := $(shell git describe --tags --exact-match 2>/dev/null || echo "$$(git describe --tags --abbrev=0 2>/dev/null || echo v0.0.0)-dev-$$(git rev-parse --short HEAD)")

build:
	go build -ldflags="-s -w -X main.version=$(VERSION)" -o aws-snapshot .

test: coverage.out

coverage.out: $(shell find . -name '*.go')
	go test -coverprofile=coverage.out ./...

fmt:
	gofmt -w .

lint:
	go vet ./...

clean:
	rm -f aws-snapshot coverage.out

show-coverage: coverage.out
	go tool cover -html=coverage.out
