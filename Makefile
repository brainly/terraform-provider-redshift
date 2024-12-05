TEST?=$$(go list ./... |grep -v 'vendor')
GOFMT_FILES?=$$(find . -name '*.go' |grep -v vendor)

default: help

.PHONY: build
build: fmt ## Build and install provider binary
	go install

.PHONY: test
test: fmt vet ## Run unit tests
	go test $(TESTARGS) ./redshift

.PHONY: testacc
testacc: fmt ## Run acceptance tests
	TF_ACC=1  go test $(TEST) -v $(TESTARGS) -count=1 -timeout 120m

.PHONY: vet
vet: ## Run go vet command
	@echo "go vet ."
	@go vet $$(go list ./... | grep -v vendor/) ; if [ $$? -eq 1 ]; then \
		echo ""; \
		echo "Vet found suspicious constructs. Please check the reported constructs"; \
		echo "and fix them if necessary before submitting the code for review."; \
		exit 1; \
	fi

.PHONY: fmt
fmt: ## Run gofmt command
	gofmt -w $(GOFMT_FILES)

.PHONY: changelog
changelog: ## Generate CHANGELOG
	@test $${RELEASE_VERSION?Please set environment variable RELEASE_VERSION}
	@test $${CHANGELOG_GITHUB_TOKEN?Please set environment variable CHANGELOG_GITHUB_TOKEN}
	@docker run -it --rm \
		-v $$PWD:/usr/local/src/your-app \
		-e CHANGELOG_GITHUB_TOKEN=$$CHANGELOG_GITHUB_TOKEN \
		ferrarimarco/github-changelog-generator \
		--user brainly \
		--project terraform-provider-redshift \
		--future-release $$RELEASE_VERSION
	@git add CHANGELOG.md && git commit -m "Release $$RELEASE_VERSION"

.PHONY: release
release: ## Release new provider version
	@test $${RELEASE_VERSION?Please set environment variable RELEASE_VERSION}
	@git tag $$RELEASE_VERSION
	@git push origin $$RELEASE_VERSION

.PHONY: doc
doc: ## Generate documentation files
	@go generate

.PHONY: help
help: ## Show this help message
	@grep -Eh '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

