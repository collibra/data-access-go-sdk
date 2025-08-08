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

type AccessProviderClient struct {
	client graphql.Client
}

func NewAccessProviderClient(client graphql.Client) AccessProviderClient {
	return AccessProviderClient{
		client: client,
	}
}

// CreateAccessProvider creates a new AccessProvider in Raito Cloud.
// The valid AccessProvider is returned if the creation is successful.
// Otherwise, an error is returned
func (a *AccessProviderClient) CreateAccessProvider(ctx context.Context, ap types.AccessProviderInput) (*types.AccessProvider, error) {
	result, err := schema.CreateAccessProvider(ctx, a.client, ap)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.CreateAccessProvider.(type) {
	case *schema.CreateAccessProviderCreateAccessProvider:
		return &response.AccessProvider, nil
	case *schema.CreateAccessProviderCreateAccessProviderAccessProviderWithOptionalAccessRequests:
		return &response.AccessProvider.AccessProvider, nil
	case *schema.CreateAccessProviderCreateAccessProviderPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("createAccessProvider", response.Message)
	case *schema.CreateAccessProviderCreateAccessProviderInvalidInputError:
		return nil, types.NewErrInvalidInput(response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", result.CreateAccessProvider)
	}
}

type UpdateAccessProviderOptions struct {
	overrideLocks bool
}

func WithAccessProviderOverrideLocks() func(options *UpdateAccessProviderOptions) {
	return func(options *UpdateAccessProviderOptions) {
		options.overrideLocks = true
	}
}

// UpdateAccessProvider updates an existing AccessProvider in Raito Cloud.
// The updated AccessProvider is returned if the update is successful.
// Otherwise, an error is returned.
func (a *AccessProviderClient) UpdateAccessProvider(ctx context.Context, id string, ap schema.AccessProviderInput, ops ...func(options *UpdateAccessProviderOptions)) (*types.AccessProvider, error) {
	options := UpdateAccessProviderOptions{}
	for _, op := range ops {
		op(&options)
	}

	result, err := schema.UpdateAccessProvider(ctx, a.client, id, ap, &options.overrideLocks)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.UpdateAccessProvider.(type) {
	case *schema.UpdateAccessProviderUpdateAccessProvider:
		return &response.AccessProvider, nil
	case *schema.UpdateAccessProviderUpdateAccessProviderAccessProviderWithOptionalAccessRequests:
		return &response.AccessProvider.AccessProvider, nil
	case *schema.UpdateAccessProviderUpdateAccessProviderPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("updateAccessProvider", response.Message)
	case *schema.UpdateAccessProviderUpdateAccessProviderInvalidInputError:
		return nil, types.NewErrInvalidInput(response.Message)
	case *schema.UpdateAccessProviderUpdateAccessProviderNotFoundError:
		return nil, types.NewErrNotFound(id, response.Typename, response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", result.UpdateAccessProvider)
	}
}

// DeleteAccessProvider deletes an existing AccessProvider in Raito Cloud.
// If the deletion is successful, nil is returned.
// Otherwise, an error is returned.
func (a *AccessProviderClient) DeleteAccessProvider(ctx context.Context, id string, ops ...func(options *UpdateAccessProviderOptions)) error {
	options := UpdateAccessProviderOptions{}
	for _, op := range ops {
		op(&options)
	}

	result, err := schema.DeleteAccessProvider(ctx, a.client, id, &options.overrideLocks)
	if err != nil {
		return types.NewErrClient(err)
	}

	switch response := result.DeleteAccessProvider.(type) {
	case *schema.DeleteAccessProviderDeleteAccessProvider:
		return nil
	case *schema.DeleteAccessProviderDeleteAccessProviderPermissionDeniedError:
		return types.NewErrPermissionDenied("deleteAccessProvider", response.Message)
	case *schema.DeleteAccessProviderDeleteAccessProviderNotFoundError:
		return types.NewErrNotFound(id, response.Typename, response.Message)
	case *schema.DeleteAccessProviderDeleteAccessProviderInvalidInputError:
		return types.NewErrInvalidInput(response.Message)
	default:
		return fmt.Errorf("unexpected response type: %T", result.DeleteAccessProvider)
	}
}

func (a *AccessProviderClient) ActivateAccessProvider(ctx context.Context, id string) (*types.AccessProvider, error) {
	result, err := schema.ActivateAccessProvider(ctx, a.client, id)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.ActivateAccessProvider.(type) {
	case *schema.ActivateAccessProviderActivateAccessProvider:
		return &response.AccessProvider, nil
	case *schema.ActivateAccessProviderActivateAccessProviderNotFoundError:
		return nil, types.NewErrNotFound(id, response.Typename, response.Message)
	case *schema.ActivateAccessProviderActivateAccessProviderPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("activateAccessProvider", response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", result.ActivateAccessProvider)
	}
}

func (a *AccessProviderClient) DeactivateAccessProvider(ctx context.Context, id string) (*types.AccessProvider, error) {
	result, err := schema.DeactivateAccessProvider(ctx, a.client, id)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.DeactivateAccessProvider.(type) {
	case *schema.DeactivateAccessProviderDeactivateAccessProvider:
		return &response.AccessProvider, nil
	case *schema.DeactivateAccessProviderDeactivateAccessProviderNotFoundError:
		return nil, types.NewErrNotFound(id, response.Typename, response.Message)
	case *schema.DeactivateAccessProviderDeactivateAccessProviderPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("deactivateAccessProvider", response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", result.DeactivateAccessProvider)
	}
}

// GetAccessProvider returns a specific AccessProvider
func (a *AccessProviderClient) GetAccessProvider(ctx context.Context, id string) (*types.AccessProvider, error) {
	result, err := schema.GetAccessProvider(ctx, a.client, id)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch ap := result.AccessProvider.(type) {
	case *schema.GetAccessProviderAccessProvider:
		return &ap.AccessProvider, nil
	case *schema.GetAccessProviderAccessProviderNotFoundError:
		return nil, types.NewErrNotFound(id, ap.Typename, ap.Message)
	case *schema.GetAccessProviderAccessProviderPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("getAccessProvider", ap.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", result.AccessProvider)
	}
}

type AccessProviderListOptions struct {
	order  []types.AccessProviderOrderByInput
	filter *types.AccessProviderFilterInput
}

// WithAccessProviderListOrder can be used to specify the order of the returned AccessProviders.
func WithAccessProviderListOrder(input ...types.AccessProviderOrderByInput) func(options *AccessProviderListOptions) {
	return func(options *AccessProviderListOptions) {
		options.order = append(options.order, input...)
	}
}

// WithAccessProviderListFilter can be used to filter the returned AccessProviders.
func WithAccessProviderListFilter(input *types.AccessProviderFilterInput) func(options *AccessProviderListOptions) {
	return func(options *AccessProviderListOptions) {
		options.filter = input
	}
}

// ListAccessProviders returns a list of AccessProviders in Raito Cloud.
// The order of the list can be specified with WithAccessProviderListOrder.
// A filter can be specified with WithAccessProviderListFilter.
// A channel is returned that can be used to receive the list of AccessProviders.
// To close the channel ensure to cancel the context.
func (a *AccessProviderClient) ListAccessProviders(ctx context.Context, ops ...func(*AccessProviderListOptions)) <-chan types.ListItem[types.AccessProvider] {
	options := AccessProviderListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*schema.PageInfo, []schema.AccessProviderPageEdgesEdge, error) {
		output, err := schema.ListAccessProviders(ctx, a.client, cursor, ptr.Int(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch page := output.AccessProviders.(type) {
		case *schema.ListAccessProvidersAccessProvidersPagedResult:
			return &page.PageInfo.PageInfo, page.Edges, nil
		case *schema.ListAccessProvidersAccessProvidersPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("listAccessProviders", page.Message)
		default:
			return nil, nil, errors.New("unreachable")
		}
	}

	edgeFn := func(edge *schema.AccessProviderPageEdgesEdge) (*string, *schema.AccessProvider, error) {
		cursor := edge.Cursor

		if edge.Node == nil {
			return cursor, nil, nil
		}

		listItem := (*edge.Node).(*schema.AccessProviderPageEdgesEdgeNodeAccessProvider)

		return cursor, &listItem.AccessProvider, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, edgeFn)
}

type AccessProviderWhoListOptions struct {
	order []types.AccessProviderWhoOrderByInput
}

// WithAccessProviderWhoListOrder can be used to specify the order of the returned AccessProviderWhoList
func WithAccessProviderWhoListOrder(input ...schema.AccessProviderWhoOrderByInput) func(options *AccessProviderWhoListOptions) {
	return func(options *AccessProviderWhoListOptions) {
		options.order = append(options.order, input...)
	}
}

// GetAccessProviderWhoList returns all who items of an AccessProvider in Raito Cloud.
// The order of the list can be specified with WithAccessProviderWhoListOrder.
// A channel is returned that can be used to receive the list of AccessProviderWhoListItem.
// To close the channel ensure to cancel the context.
func (a *AccessProviderClient) GetAccessProviderWhoList(ctx context.Context, id string, ops ...func(*AccessProviderWhoListOptions)) <-chan types.ListItem[types.AccessProviderWhoListItem] {
	options := AccessProviderWhoListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.AccessProviderWhoListEdgesEdge, error) {
		output, err := schema.GetAccessProviderWhoList(ctx, a.client, id, cursor, ptr.Int(internal.MaxPageSize), nil, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch ap := output.AccessProvider.(type) {
		case *schema.GetAccessProviderWhoListAccessProvider:
			switch whoList := ap.WhoList.(type) {
			case *schema.GetAccessProviderWhoListAccessProviderWhoListPagedResult:
				return &whoList.PageInfo.PageInfo, whoList.Edges, nil
			case *schema.GetAccessProviderWhoListAccessProviderWhoListPermissionDeniedError:
				return nil, nil, types.NewErrPermissionDenied("accessProviderWhoList", whoList.Message)
			}
		case *schema.GetAccessProviderWhoListAccessProviderNotFoundError:
			return nil, nil, types.NewErrNotFound(id, ap.Typename, ap.Message)
		case *schema.GetAccessProviderWhoListAccessProviderPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("accessProvider", ap.Message)
		default:
			return nil, nil, fmt.Errorf("unexpected type '%T': %w", ap, types.ErrUnknownType)
		}

		return nil, nil, errors.New("unreachable")
	}

	edgeFn := func(edge *types.AccessProviderWhoListEdgesEdge) (*string, *schema.AccessProviderWhoListItem, error) {
		cursor := edge.Cursor

		if edge.Node == nil {
			return cursor, nil, nil
		}

		listItem := (*edge.Node).(*types.AccessProviderWhoListEdgesEdgeNodeAccessWhoItem)

		return cursor, &listItem.AccessProviderWhoListItem, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, edgeFn)
}

type AccessProviderWhatListOptions struct {
	order  []types.AccessWhatOrderByInput
	filter *types.AccessWhatFilterInput
}

// WithAccessProviderWhatListOrder can be used to specify the order of the returned AccessProviderWhatList
func WithAccessProviderWhatListOrder(input ...types.AccessWhatOrderByInput) func(options *AccessProviderWhatListOptions) {
	return func(options *AccessProviderWhatListOptions) {
		options.order = append(options.order, input...)
	}
}

// WithAccessProviderWhatListFilter can be used to filter the returned AccessProviderWhatList.
func WithAccessProviderWhatListFilter(input *types.AccessWhatFilterInput) func(options *AccessProviderWhatListOptions) {
	return func(options *AccessProviderWhatListOptions) {
		options.filter = input
	}
}

// GetAccessProviderWhatDataObjectList returns all what items of an AccessProvider in Raito Cloud.
// The order of the list can be specified with WithAccessProviderWhatListOrder.
// A channel is returned that can be used to receive the list of AccessProviderWhatDataObjectListItem.
// To close the channel ensure to cancel the context.
func (a *AccessProviderClient) GetAccessProviderWhatDataObjectList(ctx context.Context, id string, ops ...func(*AccessProviderWhatListOptions)) <-chan types.ListItem[types.AccessProviderWhatListItem] {
	options := AccessProviderWhatListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.AccessProviderWhatListEdgesEdge, error) {
		output, err := schema.GetAccessProviderWhatDataObjectList(ctx, a.client, id, cursor, ptr.Int(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch ap := output.AccessProvider.(type) {
		case *schema.GetAccessProviderWhatDataObjectListAccessProvider:
			switch whatList := ap.WhatDataObjects.(type) {
			case *schema.GetAccessProviderWhatDataObjectListAccessProviderWhatDataObjectsPagedResult:
				return &whatList.PageInfo.PageInfo, whatList.Edges, nil
			case *schema.GetAccessProviderWhatDataObjectListAccessProviderWhatDataObjectsPermissionDeniedError:
				return nil, nil, types.NewErrPermissionDenied("accessProviderWhatDataObjectList", whatList.Message)
			}
		case *schema.GetAccessProviderWhatDataObjectListAccessProviderNotFoundError:
			return nil, nil, types.NewErrNotFound(id, ap.Typename, ap.Message)
		case *schema.GetAccessProviderWhatDataObjectListAccessProviderPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("accessProvider", ap.Message)
		default:
			return nil, nil, fmt.Errorf("unexpected type '%T': %w", ap, types.ErrUnknownType)
		}

		return nil, nil, errors.New("unreachable")
	}

	edgeFn := func(edge *types.AccessProviderWhatListEdgesEdge) (*string, *schema.AccessProviderWhatListItem, error) {
		cursor := edge.Cursor

		if edge.Node == nil {
			return cursor, nil, nil
		}

		listItem := (*edge.Node).(*types.AccessProviderWhatListEdgesEdgeNodeAccessWhatItem)

		return cursor, &listItem.AccessProviderWhatListItem, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, edgeFn)
}

// AccessProviderWhatAccessProviderListOptions options for listing what access providers of an AccessProvider in Raito Cloud.
type AccessProviderWhatAccessProviderListOptions struct {
	order  []types.AccessWhatOrderByInput
	filter *types.AccessProviderWhatAccessProviderFilterInput
}

// WithAccessProviderWhatAccessProviderListOrder can be used to specify the order of the returned AccessProviderWhatAccessProviderList
func WithAccessProviderWhatAccessProviderListOrder(input ...schema.AccessWhatOrderByInput) func(options *AccessProviderWhatAccessProviderListOptions) {
	return func(options *AccessProviderWhatAccessProviderListOptions) {
		options.order = append(options.order, input...)
	}
}

// WithAccessProviderWhatAccessProviderListFilter can be used to specify the filter of the returned AccessProviderWhatAccessProviderList.
func WithAccessProviderWhatAccessProviderListFilter(filter *types.AccessProviderWhatAccessProviderFilterInput) func(options *AccessProviderWhatAccessProviderListOptions) {
	return func(options *AccessProviderWhatAccessProviderListOptions) {
		options.filter = filter
	}
}

// GetAccessProviderWhatAccessProviderList returns all what access providers of an AccessProvider in Raito Cloud.
func (a *AccessProviderClient) GetAccessProviderWhatAccessProviderList(ctx context.Context, id string, ops ...func(*AccessProviderWhatAccessProviderListOptions)) <-chan types.ListItem[types.AccessWhatAccessProviderItem] {
	options := AccessProviderWhatAccessProviderListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.AccessProviderWhatAccessProviderListEdgesEdge, error) {
		output, err := schema.GetAccessProviderWhatAccessProviders(ctx, a.client, id, cursor, ptr.Int(internal.MaxPageSize), nil, options.order, options.filter)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch ap := output.AccessProvider.(type) {
		case *schema.GetAccessProviderWhatAccessProvidersAccessProvider:
			switch whatList := ap.WhatAccessProviders.(type) {
			case *schema.GetAccessProviderWhatAccessProvidersAccessProviderWhatAccessProvidersPagedResult:
				return &whatList.PageInfo.PageInfo, whatList.Edges, nil
			case *schema.GetAccessProviderWhatAccessProvidersAccessProviderWhatAccessProvidersPermissionDeniedError:
				return nil, nil, types.NewErrPermissionDenied("accessProviderWhatAccessProviderList", whatList.Message)
			default:
				return nil, nil, fmt.Errorf("unexpected type '%T': %w", ap, types.ErrUnknownType)
			}
		case *schema.GetAccessProviderWhatAccessProvidersAccessProviderNotFoundError:
			return nil, nil, types.NewErrNotFound(id, ap.Typename, ap.Message)
		case *schema.GetAccessProviderWhatAccessProvidersAccessProviderPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("accessProvider", ap.Message)
		default:
			return nil, nil, fmt.Errorf("unexpected type '%T': %w", ap, types.ErrUnknownType)
		}
	}

	edgeFn := func(edge *types.AccessProviderWhatAccessProviderListEdgesEdge) (*string, *types.AccessWhatAccessProviderItem, error) {
		cursor := edge.Cursor

		if edge.Node == nil {
			return cursor, nil, nil
		}

		listItem := (*edge.Node).(*types.AccessProviderWhatAccessProviderListEdgesEdgeNodeAccessWhatAccessProviderItem)

		return cursor, &listItem.AccessWhatAccessProviderItem, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, edgeFn)
}

type AccessProviderAbacWhatScopeListOptions struct {
	order  []types.AccessWhatOrderByInput
	search *string
}

// WithAccessProviderAbacWhatScopeListOrder can be used to specify the order of the returned AccessProviderAbacWhatScopeList.
func WithAccessProviderAbacWhatScopeListOrder(input ...types.AccessWhatOrderByInput) func(options *AccessProviderAbacWhatScopeListOptions) {
	return func(options *AccessProviderAbacWhatScopeListOptions) {
		options.order = append(options.order, input...)
	}
}

// WithAccessProviderAbacWhatScopeListSearch can be used to specify the search of the returned Access
func WithAccessProviderAbacWhatScopeListSearch(search string) func(options *AccessProviderAbacWhatScopeListOptions) {
	return func(options *AccessProviderAbacWhatScopeListOptions) {
		options.search = &search
	}
}

// GetAccessProviderAbacWhatScope returns all abac what scopes of an AccessProvider
// id is the id of the AccessProvider
// WithAccessProviderAbacWhatScopeListSearch can be used to specify the search of the returned types.DataObject
// WithAccessProviderAbacWhatScopeListOrder can be used to specify the order of the returned types.DataObject
func (a *AccessProviderClient) GetAccessProviderAbacWhatScope(ctx context.Context, id string, ops ...func(*AccessProviderAbacWhatScopeListOptions)) <-chan types.ListItem[types.DataObject] {
	options := AccessProviderAbacWhatScopeListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.AccessProviderWhatAbacScopeListEdgesEdge, error) {
		output, err := schema.ListAccessProviderAbacWhatScope(ctx, a.client, id, cursor, ptr.Int(internal.MaxPageSize), options.search, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch ap := output.AccessProvider.(type) {
		case *schema.ListAccessProviderAbacWhatScopeAccessProvider:
			switch whatList := ap.WhatAbacScope.(type) {
			case *schema.ListAccessProviderAbacWhatScopeAccessProviderWhatAbacScopePagedResult:
				return &whatList.PageInfo.PageInfo, whatList.Edges, nil
			case *schema.ListAccessProviderAbacWhatScopeAccessProviderWhatAbacScopePermissionDeniedError:
				return nil, nil, types.NewErrPermissionDenied("accessProviderWhatAbacScopeList", whatList.Message)
			default:
				return nil, nil, fmt.Errorf("unexpected type '%T': %w", whatList, types.ErrUnknownType)
			}
		case *schema.ListAccessProviderAbacWhatScopeAccessProviderPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("accessProvider", ap.Message)
		case *schema.ListAccessProviderAbacWhatScopeAccessProviderNotFoundError:
			return nil, nil, types.NewErrNotFound(id, ap.Typename, ap.Message)
		default:
			return nil, nil, fmt.Errorf("unexpected type '%T': %w", ap, types.ErrUnknownType)
		}
	}

	edgeFn := func(edge *types.AccessProviderWhatAbacScopeListEdgesEdge) (*string, *types.DataObject, error) {
		cursor := edge.Cursor

		if edge.Node == nil {
			return cursor, nil, nil
		}

		listItem := (*edge.Node).(*types.AccessProviderWhatAbacScopeListEdgesEdgeNodeDataObject)

		return cursor, &listItem.DataObject, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, edgeFn)
}
