IMAGE ?= registry.tools.thaidevops.co/pertisksoft/pertisk-kos-scaler
TAG ?= latest
VERSION ?= 0.1.0
PLATFORM ?= linux/amd64
PLATFORMS ?= linux/amd64,linux/arm64
CHART ?= helm/pertisk-kos-scaler
HELM_PACKAGE_DIR ?= helm/dist

.PHONY: help build release

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "%-12s %s\n", $$1, $$2}'

build: ## Build the container image locally
	docker buildx build --platform $(PLATFORM) --provenance=false -t $(IMAGE):$(TAG) --load .

release: ## Push the multi-arch image, then package the Helm chart
	docker buildx build --platform $(PLATFORMS) --provenance=false -t $(IMAGE):$(TAG) --push .
	mkdir -p $(HELM_PACKAGE_DIR)
	helm package $(CHART) --destination $(HELM_PACKAGE_DIR) --version $(VERSION) --app-version $(TAG)
