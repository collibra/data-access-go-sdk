package sdk

import (
	"errors"
	"fmt"
	neturl "net/url"
	"strings"
	"sync"
	"time"

	gql "github.com/Khan/genqlient/graphql"
	"github.com/hashicorp/go-retryablehttp"

	"github.com/collibra/data-access-go-sdk/internal"
	"github.com/collibra/data-access-go-sdk/services"
)

type ClientOptions = func(*internal.ClientOptions)

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
	siteClient          singletonClient[services.SiteService]
}

func WithRetryWaitMin(d time.Duration) ClientOptions {
	return func(ops *internal.ClientOptions) {
		ops.RetryWaitMin = d
	}
}

func WithRetryWaitMax(d time.Duration) ClientOptions {
	return func(ops *internal.ClientOptions) {
		ops.RetryWaitMax = d
	}
}

func WithRetryMax(retries int) ClientOptions {
	return func(ops *internal.ClientOptions) {
		ops.RetryMax = retries
	}
}

func WithLinearJitterBackoff() ClientOptions {
	return func(ops *internal.ClientOptions) {
		ops.Backoff = retryablehttp.LinearJitterBackoff
	}
}

func WithRateLimitLinearJitterBackoff() ClientOptions {
	return func(ops *internal.ClientOptions) {
		ops.Backoff = retryablehttp.RateLimitLinearJitterBackoff
	}
}

func WithUsername(username string) ClientOptions {
	return func(ops *internal.ClientOptions) {
		ops.Username = username
	}
}

func WithPassword(password string) ClientOptions {
	return func(ops *internal.ClientOptions) {
		ops.Password = password
	}
}

func WithUserAgent(userAgent string) ClientOptions {
	return func(ops *internal.ClientOptions) {
		ops.UserAgent = userAgent
	}
}

func WithLogger(logger retryablehttp.Logger) ClientOptions {
	return func(ops *internal.ClientOptions) {
		ops.Logger = logger
	}
}

func WithLeveledLogger(logger retryablehttp.LeveledLogger) ClientOptions {
	return func(ops *internal.ClientOptions) {
		ops.LeveledLogger = logger
	}
}

// NewClient creates a new CollibraClient with the given credentials.
// A non-empty, valid absolute HTTP(S) URL must be provided.
func NewClient(url string, options ...ClientOptions) (*CollibraClient, error) {
	if url == "" {
		return nil, errors.New("url must not be empty")
	}

	parsed, err := neturl.ParseRequestURI(url)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		if err == nil {
			return nil, fmt.Errorf("url '%q' must be an absolute HTTP(S) URL", url)
		}
		return nil, fmt.Errorf("url '%q' must be an absolute HTTP(S) URL: %s", url, err.Error())
	}

	ops := internal.ClientOptions{
		RetryWaitMin: 550 * time.Millisecond,
		RetryWaitMax: 30 * time.Second,
		RetryMax:     4,
		Backoff:      retryablehttp.DefaultBackoff,
	}

	for _, op := range options {
		op(&ops)
	}

	apiUrl := url
	if !strings.HasSuffix(apiUrl, "/") {
		apiUrl += "/"
	}

	if !strings.HasSuffix(apiUrl, "/"+internal.DataAccessApiPath+"/") {
		apiUrl += internal.DataAccessApiPath + "/"
	}

	gqlApiUrl := apiUrl + internal.GqlApiPath

	client := internal.CreateHttpClient(&ops)

	glcClient := gql.NewClient(gqlApiUrl, client)

	return &CollibraClient{
		accessControlClient: newSingletonClient(glcClient, services.NewAccessControlClient),
		dataObjectClient:    newSingletonClient(glcClient, services.NewDataObjectClient),
		dataSourceClient:    newSingletonClient(glcClient, services.NewDataSourceClient),
		exporterClient:      newSingletonClient(glcClient, services.NewExporterClient),
		grantCategoryClient: newSingletonClient(glcClient, services.NewGrantCategoryClient),
		importerClient:      newSingletonClient(glcClient, services.NewImporterClient),
		jobClient:           newSingletonClient(glcClient, services.NewJobClient),
		roleClient:          newSingletonClient(glcClient, services.NewRoleClient),
		userClient:          newSingletonClient(glcClient, services.NewUserClient),
		siteClient:          newSingletonClient(glcClient, services.NewSiteService),
	}, nil
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

// Site returns the SiteService
func (c *CollibraClient) Site() *services.SiteService {
	return c.siteClient.Get()
}
