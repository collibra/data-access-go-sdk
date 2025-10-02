package sdk

import (
	"net/http"
	"strings"
	"sync"
	"time"

	gql "github.com/Khan/genqlient/graphql"

	"github.com/collibra/access-governance-go-sdk/internal"
	"github.com/collibra/access-governance-go-sdk/internal/rest"
	"github.com/collibra/access-governance-go-sdk/services"
)

type singletonClient[T any] struct {
	factory func() *T
	once    sync.Once
	client  *T
}

func newSingletonClient[C any, T any](client C, clientFactory func(client C) *T) singletonClient[T] {
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
	exporterClient      singletonClient[services.ExporterClient]
	grantCategoryClient singletonClient[services.GrantCategoryClient]
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

	if !strings.HasSuffix(apiUrl, "/") {
		apiUrl += "/"
	}

	gqlApiUrl := apiUrl + internal.GqlApiPath
	restApiUrl := apiUrl + internal.RestApiPath

	authDoer := &internal.BasicAuthedDoer{
		User:     user,
		Password: password,
		Url:      apiUrl,
	}

	glcClient := gql.NewClient(gqlApiUrl, authDoer)
	restClient := rest.NewRestClient(restApiUrl, &http.Client{
		Transport: rest.DoerRoundTripWrapper{Doer: authDoer},
		Timeout:   time.Second * 30,
	})

	return &CollibraClient{
		accessControlClient: newSingletonClient(glcClient, services.NewAccessControlClient),
		dataObjectClient:    newSingletonClient(glcClient, services.NewDataObjectClient),
		dataSourceClient:    newSingletonClient(glcClient, services.NewDataSourceClient),
		exporterClient:      newSingletonClient(restClient, services.NewExporterClient),
		grantCategoryClient: newSingletonClient(glcClient, services.NewGrantCategoryClient),
		importerClient:      newSingletonClient(glcClient, services.NewImporterClient),
		jobClient:           newSingletonClient(glcClient, services.NewJobClient),
		roleClient:          newSingletonClient(glcClient, services.NewRoleClient),
		userClient:          newSingletonClient(glcClient, services.NewUserClient),
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

func (c *CollibraClient) Exporter() *services.ExporterClient {
	return c.exporterClient.Get()
}

// GrantCategory returns the GrantCategoryClient
func (c *CollibraClient) GrantCategory() *services.GrantCategoryClient {
	return c.grantCategoryClient.Get()
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
