package sdk

import (
	"context"
	"strings"

	gql "github.com/Khan/genqlient/graphql"

	"github.com/collibra/access-governance-go-sdk/internal"
	"github.com/collibra/access-governance-go-sdk/services"
)

type CollibraClient struct {
	accessControlClient services.AccessControlClient
	dataObjectClient    services.DataObjectClient
	dataSourceClient    services.DataSourceClient
	grantCategoryClient services.GrantCategoryClient
	groupClient         services.GroupClient
	identityStoreClient services.IdentityStoreClient
	roleClient          services.RoleClient
	userClient          services.UserClient
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

// NewClient creates a new CollibraClient with the given credentials.
func NewClient(ctx context.Context, domain, user, secret string, ops ...func(options *ClientOptions)) *CollibraClient {
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

	return &CollibraClient{
		accessControlClient: services.NewAccessControlClient(client),
		dataObjectClient:    services.NewDataObjectClient(client),
		dataSourceClient:    services.NewDataSourceClient(client),
		grantCategoryClient: services.NewGrantCategoryClient(client),
		groupClient:         services.NewGroupClient(client),
		identityStoreClient: services.NewIdentityStoreClient(client),
		roleClient:          services.NewRoleClient(client),
		userClient:          services.NewUserClient(client),
	}
}

// AccessControl returns the AccessControlClient
func (c *CollibraClient) AccessControl() *services.AccessControlClient {
	return &c.accessControlClient
}

// DataObject returns the DataObjectClient
func (c *CollibraClient) DataObject() *services.DataObjectClient {
	return &c.dataObjectClient
}

// DataSource returns the DataSourceClient
func (c *CollibraClient) DataSource() *services.DataSourceClient {
	return &c.dataSourceClient
}

// GrantCategory returns the GrantCategoryClient
func (c *CollibraClient) GrantCategory() *services.GrantCategoryClient {
	return &c.grantCategoryClient
}

// Group returns the GroupClient
func (c *CollibraClient) Group() *services.GroupClient {
	return &c.groupClient
}

// IdentityStore returns the IdentityStoreClient
func (c *CollibraClient) IdentityStore() *services.IdentityStoreClient {
	return &c.identityStoreClient
}

// Role returns the RoleClient
func (c *CollibraClient) Role() *services.RoleClient {
	return &c.roleClient
}

// User returns the UserClient
func (c *CollibraClient) User() *services.UserClient {
	return &c.userClient
}
