package services

import (
	"context"
	"errors"
	"fmt"

	"github.com/Khan/genqlient/graphql"
	"github.com/aws/smithy-go/ptr"

	"github.com/raito-io/sdk-go/internal"
	"github.com/raito-io/sdk-go/internal/schema"
	"github.com/raito-io/sdk-go/types"
)

type DataSourceClient struct {
	client graphql.Client
}

func NewDataSourceClient(client graphql.Client) DataSourceClient {
	return DataSourceClient{
		client: client,
	}
}

// CreateDataSource creates a new DataSource.
// Returns the newly created DataSource if successful.
// Otherwise, returns an error.
func (c *DataSourceClient) CreateDataSource(ctx context.Context, ds types.DataSourceInput) (*types.DataSource, error) {
	result, err := schema.CreateDataSource(ctx, c.client, ds)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.CreateDataSource.(type) {
	case *schema.CreateDataSourceCreateDataSource:
		return &response.DataSource, nil
	case *schema.CreateDataSourceCreateDataSourceNotFoundError:
		return nil, types.NewErrNotFound("", response.Typename, response.Message)
	case *schema.CreateDataSourceCreateDataSourcePermissionDeniedError:
		return nil, types.NewErrPermissionDenied("createDataSource", response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", result.CreateDataSource)
	}
}

// UpdateDataSource updates an existing DataSource.
// Returns the updated DataSource if successful.
// Otherwise, returns an error.
func (c *DataSourceClient) UpdateDataSource(ctx context.Context, id string, ds types.DataSourceInput) (*types.DataSource, error) {
	result, err := schema.UpdateDataSource(ctx, c.client, id, ds)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.UpdateDataSource.(type) {
	case *schema.UpdateDataSourceUpdateDataSource:
		return &response.DataSource, nil
	case *schema.UpdateDataSourceUpdateDataSourceNotFoundError:
		return nil, types.NewErrNotFound(id, response.Typename, response.Message)
	case *schema.UpdateDataSourceUpdateDataSourcePermissionDeniedError:
		return nil, types.NewErrPermissionDenied("updateDataSource", response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", result.UpdateDataSource)
	}
}

// DeleteDataSource deletes an existing DataSource.
// Returns nil if successful.
// Otherwise, returns an error.
func (c *DataSourceClient) DeleteDataSource(ctx context.Context, id string) error {
	result, err := schema.DeleteDataSource(ctx, c.client, id)
	if err != nil {
		return types.NewErrClient(err)
	}

	switch response := result.DeleteDataSource.(type) {
	case *schema.DeleteDataSourceDeleteDataSource:
		return nil
	case *schema.DeleteDataSourceDeleteDataSourcePermissionDeniedError:
		return types.NewErrPermissionDenied("deleteDataSource", response.Message)
	default:
		return fmt.Errorf("unexpected response type: %T", result.DeleteDataSource)
	}
}

// AddIdentityStoreToDataSource adds an existing IdentityStore to an existing DataSource.
// Returns nil if successful.
// Otherwise, returns an error.
func (c *DataSourceClient) AddIdentityStoreToDataSource(ctx context.Context, dsId string, isId string) error {
	result, err := schema.AddIdentityStoreToDataSource(ctx, c.client, dsId, isId)
	if err != nil {
		return types.NewErrClient(err)
	}

	switch response := result.AddIdentityStoreToDataSource.(type) {
	case *schema.AddIdentityStoreToDataSourceAddIdentityStoreToDataSource:
		return nil
	case *schema.AddIdentityStoreToDataSourceAddIdentityStoreToDataSourceNotFoundError:
		return types.NewErrNotFound(dsId, response.Typename, response.Message)
	case *schema.AddIdentityStoreToDataSourceAddIdentityStoreToDataSourcePermissionDeniedError:
		return types.NewErrPermissionDenied("addIdentityStoreToDataSource", response.Message)
	default:
		return fmt.Errorf("unexpected response type: %T", result.AddIdentityStoreToDataSource)
	}
}

func (c *DataSourceClient) RemoveIdentityStoreFromDataSource(ctx context.Context, dsId string, isId string) error {
	result, err := schema.RemoveIdentityStoreFromDataSource(ctx, c.client, dsId, isId)
	if err != nil {
		return types.NewErrClient(err)
	}

	switch response := result.RemoveIdentityStoreFromDataSource.(type) {
	case *schema.RemoveIdentityStoreFromDataSourceRemoveIdentityStoreFromDataSource:
		return nil
	case *schema.RemoveIdentityStoreFromDataSourceRemoveIdentityStoreFromDataSourceNotFoundError:
		return types.NewErrNotFound(dsId, response.Typename, response.Message)
	case *schema.RemoveIdentityStoreFromDataSourceRemoveIdentityStoreFromDataSourcePermissionDeniedError:
		return types.NewErrPermissionDenied("removeIdentityStoreFromDataSource", response.Message)
	default:
		return fmt.Errorf("unexpected response type: %T", result.RemoveIdentityStoreFromDataSource)
	}
}

// GetDataSource returns an existing DataSource.
func (c *DataSourceClient) GetDataSource(ctx context.Context, id string) (*types.DataSource, error) {
	result, err := schema.GetDataSource(ctx, c.client, id)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch ds := result.DataSource.(type) {
	case *schema.GetDataSourceDataSource:
		return &ds.DataSource, nil
	case *schema.GetDataSourceDataSourcePermissionDeniedError:
		return nil, types.NewErrPermissionDenied("dataSource", ds.Message)
	case *schema.GetDataSourceDataSourceNotFoundError:
		return nil, types.NewErrNotFound(id, ds.Typename, ds.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", result.DataSource)
	}
}

// GetMaskingMetadata Get masking information for a DataSource
func (c *DataSourceClient) GetMaskingMetadata(ctx context.Context, id string) (*types.MaskingMetadata, error) {
	result, err := schema.DataSourceMaskInformation(ctx, c.client, id)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch ds := result.DataSource.(type) {
	case *schema.DataSourceMaskInformationDataSource:
		if ds.MaskingMetadata != nil {
			return &ds.MaskingMetadata.MaskingMetadata, nil
		} else {
			return nil, nil
		}
	case *schema.DataSourceMaskInformationDataSourcePermissionDeniedError:
		return nil, types.NewErrPermissionDenied("dataSource", ds.Message)
	case *schema.DataSourceMaskInformationDataSourceNotFoundError:
		return nil, types.NewErrNotFound(id, ds.Typename, ds.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", result.DataSource)
	}
}

// DataSourceListOptions list options for listing DataSources.
type DataSourceListOptions struct {
	order  []types.DataSourceOrderByInput
	filter *types.DataSourceFilterInput
	search *string
}

// WithDataSourceListOrder sets the order of the returned DataSources in the ListDataSources call.
func WithDataSourceListOrder(input ...types.DataSourceOrderByInput) func(options *DataSourceListOptions) {
	return func(options *DataSourceListOptions) {
		options.order = append(options.order, input...)
	}
}

// WithDataSourceListFilter sets the filter of the returned DataSources in the ListDataSources call.
func WithDataSourceListFilter(input *types.DataSourceFilterInput) func(options *DataSourceListOptions) {
	return func(options *DataSourceListOptions) {
		options.filter = input
	}
}

// WithDataSourceListSearch sets the search query of the returned DataSources in the ListDataSources call.
func WithDataSourceListSearch(input *string) func(options *DataSourceListOptions) {
	return func(options *DataSourceListOptions) {
		options.search = input
	}
}

// ListDataSources return a list of DataSources
// The order of the list can be specified with WithDataSourceListOrder.
// A filter can be specified with WithDataSourceListFilter.
// A channel is returned that can be used to receive the list of DataSourceListItem.
// To close the channel ensure to cancel the context.
func (c *DataSourceClient) ListDataSources(ctx context.Context, ops ...func(*DataSourceListOptions)) <-chan types.ListItem[types.DataSource] {
	options := DataSourceListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.DataSourcePageEdgesEdge, error) {
		output, err := schema.ListDataSources(ctx, c.client, cursor, ptr.Int(internal.MaxPageSize), options.filter, nil, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch page := output.DataSources.(type) {
		case *schema.ListDataSourcesDataSourcesPagedResult:
			return &page.PageInfo.PageInfo, page.Edges, nil
		case *schema.ListDataSourcesDataSourcesPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("listDataSources", page.Message)
		}

		return nil, nil, errors.New("unreachable")
	}

	edgeFn := func(edge *types.DataSourcePageEdgesEdge) (*string, *schema.DataSource, error) {
		cursor := edge.Cursor

		if edge.Node == nil {
			return cursor, nil, nil
		}

		listItem := (*edge.Node).(*types.DataSourcePageEdgesEdgeNodeDataSource)

		return cursor, &listItem.DataSource, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, edgeFn)
}

// ListIdentityStores returns a list of IdentityStores for a given DataSource.
func (c *DataSourceClient) ListIdentityStores(ctx context.Context, dsId string) ([]types.IdentityStore, error) {
	result, err := schema.DataSourceIdentityStores(ctx, c.client, dsId)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch datasource := result.DataSource.(type) {
	case *schema.DataSourceIdentityStoresDataSource:
		iss := make([]types.IdentityStore, len(datasource.IdentityStores))
		for i := range datasource.IdentityStores {
			iss[i] = datasource.IdentityStores[i].IdentityStore
		}

		return iss, nil
	case *schema.DataSourceIdentityStoresDataSourcePermissionDeniedError:
		return nil, types.NewErrPermissionDenied("listIdentityStores", datasource.Message)
	default:
		return nil, fmt.Errorf("unexpected type '%T': %w", datasource, types.ErrUnknownType)
	}
}
