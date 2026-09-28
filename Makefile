.PHONY: test race build up

test:
	go test -count=1 ./...

race:
	go test -count=1 -race ./...

build:
	go build -o bin/gateway ./cmd/gateway
	go build -o bin/mockprovider ./cmd/mockprovider
	go build -o bin/benchcheck ./cmd/benchcheck

up:
	docker compose up --build
