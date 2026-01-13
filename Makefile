.PHONY: gql fetch-schema fetch-local-schema fix-lint lint build test

ifeq ($(GITHUB_ACTIONS),true)
  	gotestsum := $(GO) run gotest.tools/gotestsum --format github-actions --format-hide-empty-pkg --debug
else
	gotestsum := $(GO) run gotest.tools/gotestsum --format testname --debug
endif

gql:
	go run github.com/Khan/genqlient internal/schema/genqlient.yaml
	go run github.com/collibra/data-access-go-sdk/agen --input internal/schema/generated.go --output types/generated.go

fetch-schema:
	@ .script/fetch-schema.sh --output internal/schema/schema.graphql --login ${COLLIBRA_USERNAME} --password ${COLLIBRA_PASSWORD}

fetch-local-schema:
	npx --yes @apollo/rover graph introspect http://localhost:8080/query --output internal/schema/schema.graphql

fix-lint:
	go fmt ./...
	golangci-lint run --fix ./...

lint:
	golangci-lint run ./...
	go fmt ./...

build:
	go build ./...

test:
	$(gotestsum) -- -race ./...