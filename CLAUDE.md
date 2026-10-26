# CLAUDE.md

Go SDK (package `sdk`) for the Collibra Data Access GraphQL API.

## Commands

- `make test` and `go test ./services/...` run against a live Collibra and fail unless `COLLIBRA_URL`,
  `COLLIBRA_USER` and `COLLIBRA_PASSWORD` are set. Without a server: `go test . ./internal/...` and, in `services/`, the tests
  that don't use a suite: `go test ./services/ -run 'TestMarshalSyncParameterValues|TestDataSourceListFilter'`.
- `make gql` needs `internal/schema/schema.graphql`, which is gitignored: run `make fetch-schema` first. It reads
  `COLLIBRA_URL`, `COLLIBRA_USERNAME` and `COLLIBRA_PASSWORD`; note USERNAME here against USER for the tests.
- One test: `go test ./services/ -run TestDataSourceServiceTestSuite/TestDataSources`.

## Generated code

`internal/schema/generated.go` (about 2 MB) and `types/generated.go` come from `make gql`. Never edit them and never
read them whole; grep for a symbol. The nightly `update-gql-schema.yml` workflow regenerates both and opens an
auto-merging PR, so hand edits get overwritten.

## Adding an operation

1. Write the query or mutation in `internal/schema/queries/<domain>.graphql`, spreading the error fragments from
   `errors.graphql` that the API can return (`...PermissionDeniedError`, `...NotFoundError`, `...InvalidInputError`).
2. `make gql`.
3. Add the method to `services/<domain>.go`. Wrap a transport failure in `types.NewErrClient(err)`, type-switch on the
   result union mapping each error member to `types.NewErrNotFound`, `NewErrPermissionDenied` or `NewErrInvalidInput`,
   and give the `default` case an error for the unexpected type.
4. A new domain also needs a `singletonClient` field, a constructor line and an accessor in `client.go`.
