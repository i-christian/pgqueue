BINARY_NAME=pgqueue-dash
BUILD_DIR=bin
VERSION=1.0.0
MAIN_PATH=.

# Database Configuration
PG_CONN_STRING ?= ""

.PHONY: all build run clean test help install

all: build ## Build the binary (default)

help: ## Show help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Compile the TUI binary
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	go build -ldflags="-s -w" -o $(BUILD_DIR)/$(BINARY_NAME) $(MAIN_PATH)
	@echo "Build complete: $(BUILD_DIR)/$(BINARY_NAME)"

run: build ## Build and run the TUI
	@echo "Starting TUI..."
	PG_CONN_STRING=$(PG_CONN_STRING) ./$(BUILD_DIR)/$(BINARY_NAME)

test: ## Run tests
	go test -v ./...

clean: ## Remove build artifacts
	@echo "Cleaning build directory..."
	rm -rf $(BUILD_DIR)

install: build ## Install the binary to your global GOBIN path
	@echo "Installing to $(shell go env GOPATH)/bin..."
	@cp $(BUILD_DIR)/$(BINARY_NAME) $(shell go env GOPATH)/bin/
	@echo "Success! You can now run '$(BINARY_NAME)' from any directory."

build-linux: ## Build for Linux AMD64
	GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64 $(MAIN_PATH)

