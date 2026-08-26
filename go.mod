module github.com/collibra/data-access-go-sdk

go 1.27.0

tool github.com/Khan/genqlient

require (
	// This is a temporary pseudo-version used because an official release
	// containing a necessary fix is not yet available. It will be replaced
	// with an official version once it is released.
	github.com/Khan/genqlient v0.8.2-0.20260825022638-8d029a3f38e6
	github.com/Masterminds/semver/v3 v3.5.0
	github.com/google/uuid v1.6.0
	github.com/hashicorp/go-cleanhttp v0.5.2
	github.com/hashicorp/go-retryablehttp v0.7.8
	github.com/stretchr/testify v1.12.1
	github.com/vektah/gqlparser/v2 v2.5.36
	golang.org/x/tools v0.49.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/agnivade/levenshtein v1.2.1 // indirect
	github.com/alexflint/go-arg v1.5.1 // indirect
	github.com/alexflint/go-scalar v1.2.0 // indirect
	github.com/bmatcuk/doublestar/v4 v4.6.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/mod v0.39.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
)
