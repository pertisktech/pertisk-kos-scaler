IMAGE ?= harbor.tools.pertisk.com/pertisksoft/kos-scaler
TAG ?= latest
PLATFORM ?= linux/amd64
PLATFORMS ?= linux/amd64,linux/arm64
RELEASE ?= kos-scaler
NAMESPACE ?= kos-scaler
CHART ?= helm/kos-scaler
HELM_ARGS ?=

.PHONY: help build release

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "%-12s %s\n", $$1, $$2}'

build: ## Build the container image
	docker buildx build --platform $(PLATFORM) -t $(IMAGE):$(TAG) --load .

release: ## Build and deploy the multi-architecture Helm release
	docker buildx build --platform $(PLATFORMS) -t $(IMAGE):$(TAG) --push .
	helm upgrade --install $(RELEASE) $(CHART) \
		--namespace $(NAMESPACE) \
		--create-namespace \
		--set image.repository=$(IMAGE) \
		--set image.tag=$(TAG) \
		$(HELM_ARGS)