package services

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"

	"github.com/Khan/genqlient/graphql"

	"github.com/collibra/data-access-go-sdk/internal"
	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/types"
)

type DataSourceClient struct {
	client graphql.Client
}

func NewDataSourceClient(client graphql.Client) *DataSourceClient {
	return &DataSourceClient{
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
func (c *DataSourceClient) ListDataSources(ctx context.Context, ops ...func(*DataSourceListOptions)) iter.Seq2[*types.DataSource, error] { //nolint:dupl
	options := DataSourceListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.DataSourceConnectionEdgesDataSourceEdge, error) {
		output, err := schema.ListDataSources(ctx, c.client, cursor, new(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch response := output.DataSources.(type) {
		case *schema.ListDataSourcesDataSourcesDataSourceConnection:
			return &response.PageInfo.PageInfo, response.Edges, nil
		case *schema.ListDataSourcesDataSourcesPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("listDataSources", response.Message)
		case *schema.ListDataSourcesDataSourcesInvalidInputError:
			return nil, nil, types.NewErrInvalidInput(response.Message)
		default:
			return nil, nil, fmt.Errorf("unexpected type '%T'", response)
		}
	}

	edgeFn := func(edge *types.DataSourceConnectionEdgesDataSourceEdge) (*string, *schema.DataSource, error) {
		cursor := edge.Cursor
		if edge.Node == nil {
			return cursor, nil, nil
		}
		return cursor, &edge.Node.DataSource, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, edgeFn)
}

func (c *DataSourceClient) SetDataSourceMetadata(ctx context.Context, id string, metadata types.DataSourceMetaDataInput) (*types.DataSource, error) {
	result, err := schema.SetDataSourceMetadata(ctx, c.client, id, metadata)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.SetDataSourceMetaData.(type) {
	case *schema.SetDataSourceMetadataSetDataSourceMetaDataDataSource:
		return &response.DataSource, nil
	case *schema.SetDataSourceMetadataSetDataSourceMetaDataNotFoundError:
		return nil, types.NewErrNotFound(id, response.Typename, response.Message)
	case *schema.SetDataSourceMetadataSetDataSourceMetaDataPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("setDataSourceMetadata", response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", result.SetDataSourceMetaData)
	}
}

// marshalSyncParameterValues JSON-encodes each non-nil Value into a string so the
// backend receives "true", "42", or "{...}" instead of a raw Go value.
func marshalSyncParameterValues(input types.SyncParameterValuesInput) (types.SyncParameterValuesInput, error) {
	values := make([]types.SyncParameterValueInput, len(input.Values))
	for i, v := range input.Values {
		if v.Value == nil {
			values[i] = v
			continue
		}

		b, err := json.Marshal(*v.Value)
		if err != nil {
			return types.SyncParameterValuesInput{}, fmt.Errorf("failed to marshal value for path %q: %w", v.Path, err)
		}

		jsonStr := any(string(b))
		values[i] = types.SyncParameterValueInput{Path: v.Path, Value: &jsonStr}
	}

	return types.SyncParameterValuesInput{DataSourceId: input.DataSourceId, Values: values}, nil
}

// SetSyncConfigurationParameterValues sets sync configuration parameter values for a DataSource.
// Each value's Path identifies the configuration key. Value can be any JSON-serializable type
// (bool, number, string, map, …); a nil value removes the parameter.
// Returns the updated DataSource if successful, otherwise returns an error.
func (c *DataSourceClient) SetSyncConfigurationParameterValues(ctx context.Context, input types.SyncParameterValuesInput) (*types.DataSource, error) {
	marshaled, err := marshalSyncParameterValues(input)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	result, err := schema.SetSyncConfigurationParameterValues(ctx, c.client, marshaled)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.SetSyncConfigurationParameterValues.(type) {
	case *schema.SetSyncConfigurationParameterValuesSetSyncConfigurationParameterValuesDataSource:
		return &response.DataSource, nil
	case *schema.SetSyncConfigurationParameterValuesSetSyncConfigurationParameterValuesNotFoundError:
		return nil, types.NewErrNotFound(input.DataSourceId, response.Typename, response.Message)
	case *schema.SetSyncConfigurationParameterValuesSetSyncConfigurationParameterValuesPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("setSyncConfigurationParameterValues", response.Message)
	case *schema.SetSyncConfigurationParameterValuesSetSyncConfigurationParameterValuesInvalidInputError:
		return nil, types.NewErrInvalidInput(response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", result.SetSyncConfigurationParameterValues)
	}
}
