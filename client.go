package sdk

import (
	"strings"
	"sync"

	gql "github.com/Khan/genqlient/graphql"

	"github.com/collibra/access-governance-go-sdk/internal"
	"github.com/collibra/access-governance-go-sdk/services"
)

type singletonClient[T any] struct {
	factory func() *T
	once    sync.Once
	client  *T
}

func newSingletonClient[T any](client gql.Client, clientFactory func(client gql.Client) *T) singletonClient[T] {
	return singletonClient[T]{
		factory: func() *T {
			return clientFactory(client)
		},
	}
}

func (s *singletonClient[T]) Get() *T {
	s.once.Do(func() {
		s.client = s.factory()
	})

	return s.client
}

type CollibraClient struct {
	accessControlClient singletonClient[services.AccessControlClient]
	dataObjectClient    singletonClient[services.DataObjectClient]
	dataSourceClient    singletonClient[services.DataSourceClient]
	grantCategoryClient singletonClient[services.GrantCategoryClient]
	groupClient         singletonClient[services.GroupClient]
	identityStoreClient singletonClient[services.IdentityStoreClient]
	importerClient      singletonClient[services.ImporterClient]
	jobClient           singletonClient[services.JobClient]
	roleClient          singletonClient[services.RoleClient]
	userClient          singletonClient[services.UserClient]
}

// NewClient creates a new CollibraClient with the given credentials.
func NewClient(user, password, url string) *CollibraClient {
	apiUrl := url
	if apiUrl == "" {
		apiUrl = internal.DefaultApiEndpoint
	}

	if !strings.HasSuffix(url, "/") {
		apiUrl += "/"
	}

	apiUrl += internal.GqlApiPath

	client := gql.NewClient(apiUrl, &internal.BasicAuthedDoer{
		User:     user,
		Password: password,
		Url:      apiUrl,
	})

	return &CollibraClient{
		accessControlClient: newSingletonClient(client, services.NewAccessControlClient),
		dataObjectClient:    newSingletonClient(client, services.NewDataObjectClient),
		dataSourceClient:    newSingletonClient(client, services.NewDataSourceClient),
		grantCategoryClient: newSingletonClient(client, services.NewGrantCategoryClient),
		groupClient:         newSingletonClient(client, services.NewGroupClient),
		identityStoreClient: newSingletonClient(client, services.NewIdentityStoreClient),
		importerClient:      newSingletonClient(client, services.NewImporterClient),
		jobClient:           newSingletonClient(client, services.NewJobClient),
		roleClient:          newSingletonClient(client, services.NewRoleClient),
		userClient:          newSingletonClient(client, services.NewUserClient),
	}
}

// AccessControl returns the AccessControlClient
func (c *CollibraClient) AccessControl() *services.AccessControlClient {
	return c.accessControlClient.Get()
}

// DataObject returns the DataObjectClient
func (c *CollibraClient) DataObject() *services.DataObjectClient {
	return c.dataObjectClient.Get()
}

// DataSource returns the DataSourceClient
func (c *CollibraClient) DataSource() *services.DataSourceClient {
	return c.dataSourceClient.Get()
}

// GrantCategory returns the GrantCategoryClient
func (c *CollibraClient) GrantCategory() *services.GrantCategoryClient {
	return c.grantCategoryClient.Get()
}

// Group returns the GroupClient
func (c *CollibraClient) Group() *services.GroupClient {
	return c.groupClient.Get()
}

// IdentityStore returns the IdentityStoreClient
func (c *CollibraClient) IdentityStore() *services.IdentityStoreClient {
	return c.identityStoreClient.Get()
}

// Importer returns the ImporterClient
func (c *CollibraClient) Importer() *services.ImporterClient {
	return c.importerClient.Get()
}

// Job returns the JobClient
func (c *CollibraClient) Job() *services.JobClient {
	return c.jobClient.Get()
}

// Role returns the RoleClient
func (c *CollibraClient) Role() *services.RoleClient {
	return c.roleClient.Get()
}

// User returns the UserClient
func (c *CollibraClient) User() *services.UserClient {
	return c.userClient.Get()
}
