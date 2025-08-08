package services

import (
	"context"
	"errors"

	"github.com/Khan/genqlient/graphql"
	"github.com/aws/smithy-go/ptr"

	"github.com/raito-io/sdk-go/internal"
	"github.com/raito-io/sdk-go/internal/schema"
	"github.com/raito-io/sdk-go/types"
)

type DataObjectClient struct {
	client graphql.Client
}

func NewDataObjectClient(client graphql.Client) DataObjectClient {
	return DataObjectClient{
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
func (c *DataObjectClient) ListDataObjects(ctx context.Context, ops ...func(options *DataObjectListOptions)) <-chan types.ListItem[types.DataObject] { //nolint:dupl
	options := DataObjectListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.DataObjectPageEdgesEdge, error) {
		output, err := schema.ListDataObjects(ctx, c.client, cursor, ptr.Int(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		return &output.DataObjects.PageInfo.PageInfo, output.DataObjects.Edges, nil
	}

	edgeFn := func(edge *types.DataObjectPageEdgesEdge) (*string, *schema.DataObject, error) {
		cursor := edge.Cursor

		if edge.Node == nil {
			return cursor, nil, nil
		}

		listItem := (*edge.Node).(*types.DataObjectPageEdgesEdgeNodeDataObject)

		return cursor, &listItem.DataObject, nil
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
func (c *DataObjectClient) GetDataObjectIdByName(ctx context.Context, fullname string, dataSource string, ops ...func(options *DataObjectByExternalIdOptions)) (string, error) {
	options := DataObjectByExternalIdOptions{}
	for _, op := range ops {
		op(&options)
	}

	result, err := schema.DataObjectByExternalId(ctx, c.client, fullname, dataSource, options.IncludeDataSource)
	if err != nil {
		return "", types.NewErrClient(err)
	}

	if len(result.DataObjects.Edges) != 1 || result.DataObjects.Edges[0].Node == nil {
		return "", errors.New("unexpected number of results")
	}

	return (*result.DataObjects.Edges[0].Node).(*schema.DataObjectByExternalIdDataObjectsPagedResultEdgesEdgeNodeDataObject).Id, nil
}
