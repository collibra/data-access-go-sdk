package sdk

import (
	"context"
	"strings"

	gql "github.com/Khan/genqlient/graphql"

	"github.com/raito-io/sdk-go/internal"
	"github.com/raito-io/sdk-go/services"
)

type RaitoClient struct {
	accessProviderClient services.AccessProviderClient
	dataObjectClient     services.DataObjectClient
	dataSourceClient     services.DataSourceClient
	grantCategoryClient  services.GrantCategoryClient
	groupClient          services.GroupClient
	identityStoreClient  services.IdentityStoreClient
	roleClient           services.RoleClient
	userClient           services.UserClient
}

type ClientOptions struct {
	UrlOverride string
}

// WithUrlOverride can be used to override the URL used to communicate with the Raito API.
func WithUrlOverride(urlOverride string) func(options *ClientOptions) {
	return func(options *ClientOptions) {
		options.UrlOverride = urlOverride
	}
}

// NewClient creates a new RaitoClient with the given credentials.
func NewClient(ctx context.Context, domain, user, secret string, ops ...func(options *ClientOptions)) *RaitoClient {
	options := ClientOptions{
		UrlOverride: internal.DefaultApiEndpoint,
	}

	for _, op := range ops {
		op(&options)
	}

	url := options.UrlOverride
	if !strings.HasSuffix(url, "/") {
		url += "/"
	}

	url += internal.GqlApiPath

	client := gql.NewClient(url, &internal.AuthedDoer{
		Domain: domain,
		User:   user,
		Secret: secret,
		Url:    options.UrlOverride,
	})

	return &RaitoClient{
		accessProviderClient: services.NewAccessProviderClient(client),
		dataObjectClient:     services.NewDataObjectClient(client),
		dataSourceClient:     services.NewDataSourceClient(client),
		grantCategoryClient:  services.NewGrantCategoryClient(client),
		groupClient:          services.NewGroupClient(client),
		identityStoreClient:  services.NewIdentityStoreClient(client),
		roleClient:           services.NewRoleClient(client),
		userClient:           services.NewUserClient(client),
	}
}

// AccessProvider returns the AccessProviderClient
func (c *RaitoClient) AccessProvider() *services.AccessProviderClient {
	return &c.accessProviderClient
}

// DataObject returns the DataObjectClient
func (c *RaitoClient) DataObject() *services.DataObjectClient {
	return &c.dataObjectClient
}

// DataSource returns the DataSourceClient
func (c *RaitoClient) DataSource() *services.DataSourceClient {
	return &c.dataSourceClient
}

// GrantCategory returns the GrantCategoryClient
func (c *RaitoClient) GrantCategory() *services.GrantCategoryClient {
	return &c.grantCategoryClient
}

// Group returns the GroupClient
func (c *RaitoClient) Group() *services.GroupClient {
	return &c.groupClient
}

// IdentityStore returns the IdentityStoreClient
func (c *RaitoClient) IdentityStore() *services.IdentityStoreClient {
	return &c.identityStoreClient
}

// Role returns the RoleClient
func (c *RaitoClient) Role() *services.RoleClient {
	return &c.roleClient
}

// User returns the UserClient
func (c *RaitoClient) User() *services.UserClient {
	return &c.userClient
}
