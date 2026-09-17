.PHONY: up down logs test build lint bench

up:
	docker compose -f deploy/docker-compose.yml up -d --build

down:
	docker compose -f deploy/docker-compose.yml down

logs:
	docker compose -f deploy/docker-compose.yml logs -f

build:
	go build ./...

test:
	go test ./...

lint:
	go vet ./...
	GOROOT="$$(go env GOROOT)" golangci-lint run

bench:
	go test -bench=. -benchmem ./...
