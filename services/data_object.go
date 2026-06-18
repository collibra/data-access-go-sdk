package services

import (
	"context"
	"fmt"
	"iter"

	"github.com/Khan/genqlient/graphql"

	"github.com/collibra/data-access-go-sdk/internal"
	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/types"
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
		output, err := schema.ListDataObjects(ctx, c.client, cursor, new(internal.MaxPageSize), options.filter, options.order)
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

type DataObjectAccessListOptions struct {
	order  []types.DataAccessReturnItemOrderByInput
	filter *types.AccessFilterInput
}

// WithDataObjectAccessListOrder sets the order of the items returned by GetDataObjectAccessList.
func WithDataObjectAccessListOrder(input ...types.DataAccessReturnItemOrderByInput) func(*DataObjectAccessListOptions) {
	return func(options *DataObjectAccessListOptions) {
		options.order = append(options.order, input...)
	}
}

// WithDataObjectAccessListFilter sets the filter applied by GetDataObjectAccessList.
func WithDataObjectAccessListFilter(input *types.AccessFilterInput) func(*DataObjectAccessListOptions) {
	return func(options *DataObjectAccessListOptions) {
		options.filter = input
	}
}

// GetDataObjectAccessList returns the access grants on the data object, one item per user
// that has access. Each item carries the permissions granted and the AccessControls
// that grant them (in trimmed form: id, name, action, state, category).
//
// To answer "does user Y have access to this data object and through which roles?", iterate
// the result and match item.User.Id against Y. For the current caller, resolve Y via
// UserClient.GetCurrentUser first. The convenience helper GetUserAccessToDataObject wraps
// this pattern.
//
// Note on ExpiresAt: the field on each item is only filled in when there is exactly one
// item in NearestAccessControls. When access is granted through multiple ACs, ExpiresAt
// will be nil and per-AC expirations are not surfaced by this query.
func (c *DataObjectClient) GetDataObjectAccessList(
	ctx context.Context,
	dataObjectID string,
	ops ...func(*DataObjectAccessListOptions),
) iter.Seq2[*types.GroupedDataAccessReturnItem, error] {
	options := DataObjectAccessListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []schema.GroupedDataAccessReturnItemConnectionEdgesGroupedDataAccessReturnItemEdge, error) {
		output, err := schema.GetDataObjectAccessList(ctx, c.client, dataObjectID, cursor, new(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch page := output.DataObject.DistinctAccess.(type) {
		case *schema.GetDataObjectAccessListDataObjectDistinctAccessGroupedDataAccessReturnItemConnection:
			return &page.PageInfo.PageInfo, page.Edges, nil
		case *schema.GetDataObjectAccessListDataObjectDistinctAccessPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("getDataObjectAccessList", page.Message)
		case *schema.GetDataObjectAccessListDataObjectDistinctAccessNotFoundError:
			return nil, nil, types.NewErrNotFound(dataObjectID, page.Typename, page.Message)
		case *schema.GetDataObjectAccessListDataObjectDistinctAccessInvalidInputError:
			return nil, nil, types.NewErrInvalidInput(page.Message)
		default:
			return nil, nil, fmt.Errorf("unexpected type '%T': %w", page, types.ErrUnknownType)
		}
	}

	edgeFn := func(edge *schema.GroupedDataAccessReturnItemConnectionEdgesGroupedDataAccessReturnItemEdge) (*string, *schema.GroupedDataAccessReturnItem, error) {
		cursor := edge.Cursor
		if edge.Node == nil {
			return cursor, nil, nil
		}
		return cursor, &edge.Node.GroupedDataAccessReturnItem, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, edgeFn)
}

// GetUserAccessToDataObject returns the access (permissions and granting AccessControls)
// that user userID has on the given data object, or nil if the user has no access.
//
// This is a convenience wrapper around GetDataObjectAccessList that iterates the result and
// matches on item.User.Id. Filter and ordering options are forwarded to the underlying call.
//
// Note on ExpiresAt: see GetDataObjectAccessList — the field is only populated when access
// is granted through a single AccessControl.
func (c *DataObjectClient) GetUserAccessToDataObject(
	ctx context.Context,
	dataObjectID string,
	userID string,
	ops ...func(*DataObjectAccessListOptions),
) (*types.GroupedDataAccessReturnItem, error) {
	for item, err := range c.GetDataObjectAccessList(ctx, dataObjectID, ops...) {
		if err != nil {
			return nil, err
		}

		if item != nil && item.User.Id == userID {
			return item, nil
		}
	}

	return nil, nil
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
