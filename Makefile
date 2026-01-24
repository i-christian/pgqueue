DB_CONTAINER=pgqueue_test_db
DB_IMAGE=postgres:18-alpine
HOST_PORT=5433
DB_DSN="postgres://user:pass@localhost:$(HOST_PORT)/task_queue_test?sslmode=disable"

.PHONY: db-up db-down test test-clean build-dash

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
	TEST_DB_DSN=$(DB_DSN) go test -v -race ./...

# Start DB, run tests, and clean up
test-full: db-down db-up
	@echo "Running integration tests..."
	@TEST_DB_DSN=$(DB_DSN) go test -v -race ./...; \
	EXIT_CODE=$$?; \
	$(MAKE) db-down; \
	exit $$EXIT_CODE

# Build the dashboard CLI
build-dash:
	go build -o ./bin/pgqueue-dash ./cmd/pgqueue-dash

# Clean build artifacts
clean:
	rm -rf ./bin

