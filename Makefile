DB_CONTAINER=pgqueue_test_db
DB_IMAGE=postgres:18-alpine
HOST_PORT=5433
DB_DSN="postgres://user:pass@localhost:$(HOST_PORT)/task_queue_test?sslmode=disable"

.PHONY: run-example db-up db-down test test-clean bench release

# Run the example code
run-example:
	@echo "Running examples..."
	go run examples/main.go

# Spin up the test database in a docker container
db-up:
	docker run --name $(DB_CONTAINER) \
		-e POSTGRES_USER=user \
		-e POSTGRES_PASSWORD=pass \
		-e POSTGRES_DB=task_queue_test \
		-p $(HOST_PORT):5432 \
		-d $(DB_IMAGE)
	@echo "Waiting for PostgreSQL to be ready..."
	@until docker exec $(DB_CONTAINER) pg_isready; do sleep 1; done

# Tear down the test database
db-down:
	docker rm -f $(DB_CONTAINER) || true

# Run all tests with a race detector
test:
	GO_ENV=test TEST_DB_DSN=$(DB_DSN) go test -v -race ./...

# Start DB, run tests, and clean up
test-full: db-down db-up
	@echo "Running integration tests..."
	@GO_ENV=test TEST_DB_DSN=$(DB_DSN) go test -v -race ./...; \
	EXIT_CODE=$$?; \
	$(MAKE) db-down; \
	exit $$EXIT_CODE

# Clean build artifacts
clean:
	rm -rf ./bin

# Run benchmarks
bench: db-down db-up
	@echo "Running performance benchmarks..."
	@GO_ENV=test TEST_DB_DSN=$(DB_DSN) go test -bench=. -benchmem ./...
	@$(MAKE) db-down

## release: Tag and push a new version (usage: make release V=0.1.0)
release:
ifndef V
	$(error version is not set. Usage: make release V=x.y.z)
endif
	@git add .
	@git commit -m "Release $(VERSION)"
	@echo "Releasing version $(V)..."
	@git tag -a v$(V) -m "Release v$(V)"
	@git push origin v$(V)
	@echo "Version v$(V) pushed to origin."
