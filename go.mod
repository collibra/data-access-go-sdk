module github.com/raito-io/sdk-go

go 1.24.0

tool (
	github.com/Khan/genqlient
	github.com/agnivade/levenshtein
	github.com/alexflint/go-arg
	github.com/alexflint/go-scalar
	github.com/raito-io/enumer
)

require (
	github.com/Khan/genqlient v0.8.0
	github.com/aws/aws-sdk-go-v2 v1.36.3
	github.com/aws/aws-sdk-go-v2/config v1.29.14
	github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider v1.52.0
	github.com/aws/smithy-go v1.22.3
	github.com/stretchr/testify v1.10.0
	golang.org/x/tools v0.33.0
)

replace github.com/Khan/genqlient v0.8.0 => github.com/raito-io/genqlient v0.0.3

require (
	github.com/agnivade/levenshtein v1.2.1 // indirect
	github.com/alexflint/go-arg v1.5.1 // indirect
	github.com/alexflint/go-scalar v1.2.0 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.17.67 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.30 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.34 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.34 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.25.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.30.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.33.19 // indirect
	github.com/bmatcuk/doublestar/v4 v4.8.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-errors/errors v1.5.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/pascaldekloe/name v1.0.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/raito-io/enumer v0.1.6 // indirect
	github.com/vektah/gqlparser/v2 v2.5.23 // indirect
	golang.org/x/mod v0.24.0 // indirect
	golang.org/x/sync v0.14.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
