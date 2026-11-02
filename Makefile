.PHONY: build test test-redis bench lint run demo clean

build:
	go build -o bin/throttle ./cmd/throttle

test:
	go test -race ./...

# Integration tests against a real Redis (docker run -p 6379:6379 redis:7-alpine)
test-redis:
	REDIS_ADDR=localhost:6379 go test -race -count=1 -cover ./...

bench:
	REDIS_ADDR=localhost:6379 go test -run '^$$' -bench . -benchmem ./pkg/limiter

lint:
	gofmt -l . | (! grep .) && go vet ./...

run: build
	THROTTLE_BACKEND=memory ./bin/throttle -config config.example.json

# Start 2 replicas + Redis and show that the limit is shared across them.
demo:
	docker compose up -d --build
	./scripts/demo.sh

clean:
	rm -rf bin
	docker compose down -v 2>/dev/null || true
