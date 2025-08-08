package services

import (
	"context"
	"fmt"

	"github.com/Khan/genqlient/graphql"
	"github.com/aws/smithy-go/ptr"

	"github.com/raito-io/sdk-go/internal"
	"github.com/raito-io/sdk-go/internal/schema"
	"github.com/raito-io/sdk-go/types"
)

type IdentityStoreClient struct {
	client graphql.Client
}

func NewIdentityStoreClient(client graphql.Client) IdentityStoreClient {
	return IdentityStoreClient{
		client: client,
	}
}

// CreateIdentityStore creates a new IdentityStore for a given DataSource.
// Returns the newly created IdentityStore if successful.
// Otherwise, returns an error.
func (c *IdentityStoreClient) CreateIdentityStore(ctx context.Context, is types.IdentityStoreInput) (*types.IdentityStore, error) {
	result, err := schema.CreateIdentityStore(ctx, c.client, is)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.CreateIdentityStore.(type) {
	case *types.CreateIdentityStoreCreateIdentityStore:
		return &response.IdentityStore, nil
	case *types.CreateIdentityStoreCreateIdentityStoreNotFoundError:
		return nil, types.NewErrNotFound("", response.Typename, response.Message)
	case *types.CreateIdentityStoreCreateIdentityStorePermissionDeniedError:
		return nil, types.NewErrPermissionDenied("createIdentityStore", response.Message)
	case *types.CreateIdentityStoreCreateIdentityStoreAlreadyExistsError:
		return nil, types.NewErrAlreadyExists("identityStore", response.Message)
	default:
		return nil, fmt.Errorf("unexpected type '%T'", response)
	}
}

// UpdateIdentityStore updates an existing IdentityStore for a given DataSource.
// Returns the updated IdentityStore if successful.
// Otherwise, returns an error.
func (c *IdentityStoreClient) UpdateIdentityStore(ctx context.Context, id string, is types.IdentityStoreInput) (*types.IdentityStore, error) {
	result, err := schema.UpdateIdentityStore(ctx, c.client, id, is)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.UpdateIdentityStore.(type) {
	case *types.UpdateIdentityStoreUpdateIdentityStore:
		return &response.IdentityStore, nil
	case *types.UpdateIdentityStoreUpdateIdentityStoreAlreadyExistsError:
		return nil, types.NewErrAlreadyExists("identityStore", response.Message)
	case *types.UpdateIdentityStoreUpdateIdentityStoreNotFoundError:
		return nil, types.NewErrNotFound(id, response.Typename, response.Message)
	case *types.UpdateIdentityStoreUpdateIdentityStorePermissionDeniedError:
		return nil, types.NewErrPermissionDenied("updateIdentityStore", response.Message)
	default:
		return nil, fmt.Errorf("unexpected type '%T'", response)
	}
}

// DeleteIdentityStore deletes an existing IdentityStore for a given DataSource.
// If successful, returns nil.
// Otherwise, returns an error.
func (c *IdentityStoreClient) DeleteIdentityStore(ctx context.Context, id string) error {
	result, err := schema.DeleteIdentityStore(ctx, c.client, id)
	if err != nil {
		return types.NewErrClient(err)
	}

	switch response := result.DeleteIdentityStore.(type) {
	case *types.DeleteIdentityStoreDeleteIdentityStore:
		return nil
	case *types.DeleteIdentityStoreDeleteIdentityStorePermissionDeniedError:
		return types.NewErrPermissionDenied("deleteIdentityStore", response.Message)
	default:
		return fmt.Errorf("unexpected type '%T'", response)
	}
}

// UpdateIdentityStoreMasterFlag updates the master flag of an existing IdentityStore for a given DataSource.
// Returns the updated IdentityStore if successful.
// Otherwise, returns an error.
func (c *IdentityStoreClient) UpdateIdentityStoreMasterFlag(ctx context.Context, id string, master bool) (*types.IdentityStore, error) {
	result, err := schema.UpdateIdentityStoreMasterFlag(ctx, c.client, id, master)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.UpdateIdentityStoreMasterFlag.(type) {
	case *types.UpdateIdentityStoreMasterFlagUpdateIdentityStoreMasterFlagIdentityStore:
		return &response.IdentityStore, nil
	case *types.UpdateIdentityStoreMasterFlagUpdateIdentityStoreMasterFlagAlreadyExistsError:
		return nil, types.NewErrAlreadyExists("identityStore", response.Message)
	case *types.UpdateIdentityStoreMasterFlagUpdateIdentityStoreMasterFlagNotFoundError:
		return nil, types.NewErrNotFound(id, response.Typename, response.Message)
	case *types.UpdateIdentityStoreMasterFlagUpdateIdentityStoreMasterFlagPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("updateIdentityStore", response.Message)
	default:
		return nil, fmt.Errorf("unexpected type '%T'", response)
	}
}

// GetIdentityStore returns an existing IdentityStore for a given DataSource.
// If successful, returns the IdentityStore.
// Otherwise, returns an error.
func (c *IdentityStoreClient) GetIdentityStore(ctx context.Context, id string) (*types.IdentityStore, error) {
	result, err := schema.GetIdentityStore(ctx, c.client, id)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.IdentityStore.(type) {
	case *types.GetIdentityStoreIdentityStore:
		return &response.IdentityStore, nil
	case *types.GetIdentityStoreIdentityStoreAlreadyExistsError:
		return nil, types.NewErrAlreadyExists("identityStore", response.Message)
	case *types.GetIdentityStoreIdentityStorePermissionDeniedError:
		return nil, types.NewErrPermissionDenied("getIdentityStore", response.Message)
	case *types.GetIdentityStoreIdentityStoreNotFoundError:
		return nil, types.NewErrNotFound(id, response.Typename, response.Message)
	default:
		return nil, fmt.Errorf("unexpected type '%T'", response)
	}
}

type ListIdentityStoresOptions struct {
	order  []schema.IdentityStoreOrderByInput
	filter *schema.IdentityStoreFilterInput
}

// WithListIdentityStoresOrder sets the order of the returned IdentityStores in the ListIdentityStores call.
func WithListIdentityStoresOrder(input ...schema.IdentityStoreOrderByInput) func(options *ListIdentityStoresOptions) {
	return func(options *ListIdentityStoresOptions) {
		options.order = append(options.order, input...)
	}
}

// WithListIdentityStoresFilter sets the filter of the returned IdentityStores in the ListIdentityStores call.
func WithListIdentityStoresFilter(input *schema.IdentityStoreFilterInput) func(options *ListIdentityStoresOptions) {
	return func(options *ListIdentityStoresOptions) {
		options.filter = input
	}
}

// ListIdentityStores returns a list of IdentityStores for a given DataSource.
// The order of the list can be specified with WithListIdentityStoresOrder.
// A filter can be specified with WithListIdentityStoresFilter.
// A channel is returned that can be used to receive the list of IdentityStores.
// To close the channel ensure to cancel the context.
func (c *IdentityStoreClient) ListIdentityStores(ctx context.Context, ops ...func(options *ListIdentityStoresOptions)) <-chan types.ListItem[types.IdentityStore] {
	options := ListIdentityStoresOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.IdentityStorePageEdgesEdge, error) {
		output, err := schema.ListIdentityStores(ctx, c.client, cursor, ptr.Int(internal.MaxPageSize), nil, options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch page := output.IdentityStores.(type) {
		case *schema.ListIdentityStoresIdentityStoresPagedResult:
			return &page.PageInfo.PageInfo, page.Edges, nil
		case *schema.ListIdentityStoresIdentityStoresPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("listIdentityStores", page.Message)
		default:
			return nil, nil, fmt.Errorf("unexpected type '%T'", page)
		}
	}

	edgeFn := func(edge *types.IdentityStorePageEdgesEdge) (*string, *types.IdentityStore, error) {
		cursor := edge.Cursor

		if edge.Node == nil {
			return cursor, nil, nil
		}

		listItem := (*edge.Node).(*types.IdentityStorePageEdgesEdgeNodeIdentityStore)

		return cursor, &listItem.IdentityStore, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, edgeFn)
}
