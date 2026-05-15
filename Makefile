.PHONY: gql fetch-schema fetch-local-schema fix-lint lint build test test-coverage

ifeq ($(GITHUB_ACTIONS),true)
  	gotestsum := go run gotest.tools/gotestsum@latest --format github-actions --format-hide-empty-pkg --debug
else
	gotestsum := go run gotest.tools/gotestsum@latest --format testname --debug
endif

gql:
	go run github.com/Khan/genqlient internal/schema/genqlient.yaml
	go run github.com/collibra/data-access-go-sdk/agen --input internal/schema/generated.go --output types/generated.go
	go run github.com/collibra/data-access-go-sdk/doctags --schema internal/schema/schema.graphql --generated internal/schema/generated.go --output internal/schema/generated.go

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

test-coverage:
	$(gotestsum) --junitfile .tests/test-results.xml --jsonfile .tests/test-results.json -- -coverpkg=./... -covermode=atomic -coverprofile=.tests/coverage.out -race -mod=readonly ./...