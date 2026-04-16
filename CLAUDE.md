# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
make build          # go build ./...
make test           # run tests with gotestsum
make test-coverage  # run tests with coverage (output in .tests/)
make lint           # run golangci-lint + go fmt check
make fix-lint       # auto-fix lint issues
make gql            # regenerate GraphQL code (genqlient + agen tool)
make fetch-schema   # fetch GraphQL schema from Collibra server (needs COLLIBRA_USERNAME, COLLIBRA_PASSWORD)
```

To run a single test:
```bash
go test ./services/... -run TestDataSourceClientTestSuite/TestCreateDataSource
```

## Architecture

This is a Go SDK (package `sdk`) for the Collibra Data Access GraphQL API. The module is `github.com/collibra/data-access-go-sdk`.

### Layers

**`client.go` (root)** — Public entry point. `NewClient(url, ...opts)` returns `*CollibraClient`. Service clients are accessed through methods like `.AccessControl()`, `.DataSource()`, etc.

**`services/`** — One file per domain (access_control, data_source, data_object, importer, exporter, job, user, roles, grant_category, site). Tests use `testify/suite`.

**`types/`** — Exported types re-aliased from `internal/schema/generated.go` via the `agen` code generator. `errors.go` defines sentinel errors.

### Key patterns

- **Functional options**: `WithUsername`, `WithPassword`, `WithLogger`, `WithMaxRetries`, etc. on `NewClient`.
- **Pagination**: Use `internal.PaginationExecutor` — takes a fetch function and returns an `iter.Seq2` iterator.
- **Error wrapping**: GraphQL client errors are wrapped in `ErrClient`. Check domain errors with `errors.Is`.
- **Adding a new operation**: write `.graphql` file in `internal/schema/queries/`, run `make gql`, add method to the relevant service in `services/`.
