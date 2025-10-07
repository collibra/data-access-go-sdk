package services

import (
	"context"
	"fmt"
	"iter"

	"github.com/Khan/genqlient/graphql"

	"github.com/collibra/access-governance-go-sdk/internal"
	"github.com/collibra/access-governance-go-sdk/internal/schema"
	"github.com/collibra/access-governance-go-sdk/types"
	"github.com/collibra/access-governance-go-sdk/utils"
)

type DataObjectClient struct {
	client graphql.Client
}

func NewDataObjectClient(client graphql.Client) *DataObjectClient {
	return &DataObjectClient{
		client: client,
	}
}

// GetDataObject returns a DataObject by id.
func (c *DataObjectClient) GetDataObject(ctx context.Context, id string) (*types.DataObject, error) {
	result, err := schema.GetDataObject(ctx, c.client, id)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	return &result.DataObject.DataObject, nil
}

type DataObjectListOptions struct {
	order  []types.DataObjectOrderByInput
	filter *types.DataObjectFilterInput
}

// WithDataObjectListOrder sets the order of the returned DataObjects in the ListDataObjects call
func WithDataObjectListOrder(input ...types.DataObjectOrderByInput) func(options *DataObjectListOptions) {
	return func(options *DataObjectListOptions) {
		options.order = append(options.order, input...)
	}
}

// WithDataObjectListFilter sets the filter of the returned DataObjects in the ListDataObjects call
func WithDataObjectListFilter(input *types.DataObjectFilterInput) func(options *DataObjectListOptions) {
	return func(options *DataObjectListOptions) {
		options.filter = input
	}
}

// ListDataObjects returns a list of DataObjects
// The order of the list can be specified with WithDataObjectListOrder
// A filter can be specified with WithDataObjectListFilter
// A channel is returned that can be used to receive the list of DataObjectListItem
// To close the channel ensure to cancel the context
func (c *DataObjectClient) ListDataObjects(ctx context.Context, ops ...func(options *DataObjectListOptions)) iter.Seq2[*types.DataObject, error] { //nolint:dupl
	options := DataObjectListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.DataObjectConnectionEdgesDataObjectEdge, error) { //nolint:dupl
		output, err := schema.ListDataObjects(ctx, c.client, cursor, utils.Ptr(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch response := (output.DataObjects).(type) {
		case *schema.ListDataObjectsDataObjectsDataObjectConnection:
			return &response.PageInfo.PageInfo, response.Edges, nil
		case *schema.ListDataObjectsDataObjectsInvalidInputError:
			return nil, nil, types.NewErrInvalidInput(response.Message)
		case *schema.ListDataObjectsDataObjectsNotFoundError:
			return nil, nil, types.NewErrNotFound("", response.Typename, response.Message)
		case *schema.ListDataObjectsDataObjectsPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("dataObjectByExternalId", response.Message)
		default:
			return nil, nil, fmt.Errorf("unexpected type '%T'", response)
		}
	}

	edgeFn := func(edge *types.DataObjectConnectionEdgesDataObjectEdge) (*string, *schema.DataObject, error) {
		cursor := edge.Cursor
		if edge.Node == nil {
			return cursor, nil, nil
		}
		return cursor, &edge.Node.DataObject, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, edgeFn)
}

type DataObjectByExternalIdOptions struct {
	IncludeDataSource bool
}

func WithDataObjectByExternalIdIncludeDataSource() func(options *DataObjectByExternalIdOptions) {
	return func(options *DataObjectByExternalIdOptions) {
		options.IncludeDataSource = true
	}
}

// GetDataObjectIdByName returns the ID of the DataObject with the given name and dataSource.
func (c *DataObjectClient) GetDataObjectIdByName(ctx context.Context, fullName string, dataSource string, ops ...func(options *DataObjectByExternalIdOptions)) (string, error) {
	options := DataObjectByExternalIdOptions{}
	for _, op := range ops {
		op(&options)
	}

	result, err := schema.DataObjectByExternalId(ctx, c.client, fullName, dataSource, options.IncludeDataSource)
	if err != nil {
		return "", types.NewErrClient(err)
	}

	switch response := (result.DataObjects).(type) {
	case *schema.DataObjectByExternalIdDataObjectsDataObjectConnection:
		if len(response.Edges) != 1 || response.Edges[0].Node == nil {
			return "", fmt.Errorf("expected 1 data object but got %d", len(response.Edges))
		}
		return response.Edges[0].Node.Id, nil
	case *schema.DataObjectByExternalIdDataObjectsInvalidInputError:
		return "", types.NewErrInvalidInput(response.Message)
	case *schema.DataObjectByExternalIdDataObjectsNotFoundError:
		return "", types.NewErrNotFound("", response.Typename, response.Message)
	case *schema.DataObjectByExternalIdDataObjectsPermissionDeniedError:
		return "", types.NewErrPermissionDenied("dataObjectByExternalId", response.Message)
	default:
		return "", fmt.Errorf("unexpected type '%T'", response)
	}
}
