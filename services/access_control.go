package services

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/Khan/genqlient/graphql"

	"github.com/collibra/data-access-go-sdk/internal"
	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/types"
)

type AccessControlClient struct {
	client graphql.Client
}

func NewAccessControlClient(client graphql.Client) *AccessControlClient {
	return &AccessControlClient{
		client: client,
	}
}

// CreateAccessControl creates a new AccessControl.
// The valid AccessControl is returned if the creation is successful.
// Otherwise, an error is returned
func (a *AccessControlClient) CreateAccessControl(ctx context.Context, ap types.AccessControlInput) (*types.AccessControl, error) {
	result, err := schema.CreateAccessControl(ctx, a.client, ap)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.CreateAccessControl.(type) {
	case *schema.CreateAccessControlCreateAccessControl:
		return &response.AccessControl, nil
	case *schema.CreateAccessControlCreateAccessControlAccessControlWithOptionalAccessRequests:
		return &response.AccessControl.AccessControl, nil
	case *schema.CreateAccessControlCreateAccessControlPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("createAccessControl", response.Message)
	case *schema.CreateAccessControlCreateAccessControlInvalidInputError:
		return nil, types.NewErrInvalidInput(response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", response)
	}
}

type UpdateAccessControlOptions struct {
	overrideLocks bool
}

func WithAccessControlOverrideLocks() func(options *UpdateAccessControlOptions) {
	return func(options *UpdateAccessControlOptions) {
		options.overrideLocks = true
	}
}

// UpdateAccessControl updates an existing AccessControl.
// The updated AccessControl is returned if the update is successful.
// Otherwise, an error is returned.
func (a *AccessControlClient) UpdateAccessControl(ctx context.Context, id string, ap schema.AccessControlInput, ops ...func(options *UpdateAccessControlOptions)) (*types.AccessControl, error) {
	options := UpdateAccessControlOptions{}
	for _, op := range ops {
		op(&options)
	}

	result, err := schema.UpdateAccessControl(ctx, a.client, id, ap, &options.overrideLocks)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.UpdateAccessControl.(type) {
	case *schema.UpdateAccessControlUpdateAccessControl:
		return &response.AccessControl, nil
	case *schema.UpdateAccessControlUpdateAccessControlAccessControlWithOptionalAccessRequests:
		return &response.AccessControl.AccessControl, nil
	case *schema.UpdateAccessControlUpdateAccessControlPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("updateAccessControl", response.Message)
	case *schema.UpdateAccessControlUpdateAccessControlInvalidInputError:
		return nil, types.NewErrInvalidInput(response.Message)
	case *schema.UpdateAccessControlUpdateAccessControlNotFoundError:
		return nil, types.NewErrNotFound(id, response.Typename, response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", response)
	}
}

// DeleteAccessControl deletes an existing AccessControl.
// If the deletion is successful, nil is returned.
// Otherwise, an error is returned.
func (a *AccessControlClient) DeleteAccessControl(ctx context.Context, id string, ops ...func(options *UpdateAccessControlOptions)) error {
	options := UpdateAccessControlOptions{}
	for _, op := range ops {
		op(&options)
	}

	result, err := schema.DeleteAccessControl(ctx, a.client, id, &options.overrideLocks)
	if err != nil {
		return types.NewErrClient(err)
	}

	switch response := result.DeleteAccessControl.(type) {
	case *schema.DeleteAccessControlDeleteAccessControl:
		return nil
	case *schema.DeleteAccessControlDeleteAccessControlPermissionDeniedError:
		return types.NewErrPermissionDenied("deleteAccessControl", response.Message)
	case *schema.DeleteAccessControlDeleteAccessControlNotFoundError:
		return types.NewErrNotFound(id, response.Typename, response.Message)
	case *schema.DeleteAccessControlDeleteAccessControlInvalidInputError:
		return types.NewErrInvalidInput(response.Message)
	default:
		return fmt.Errorf("unexpected response type: %T", response)
	}
}

func (a *AccessControlClient) ActivateAccessControl(ctx context.Context, id string) (*types.AccessControl, error) {
	result, err := schema.ActivateAccessControl(ctx, a.client, id)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.ActivateAccessControl.(type) {
	case *schema.ActivateAccessControlActivateAccessControl:
		return &response.AccessControl, nil
	case *schema.ActivateAccessControlActivateAccessControlNotFoundError:
		return nil, types.NewErrNotFound(id, response.Typename, response.Message)
	case *schema.ActivateAccessControlActivateAccessControlPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("activateAccessControl", response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", response)
	}
}

func (a *AccessControlClient) DeactivateAccessControl(ctx context.Context, id string) (*types.AccessControl, error) {
	result, err := schema.DeactivateAccessControl(ctx, a.client, id)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.DeactivateAccessControl.(type) {
	case *schema.DeactivateAccessControlDeactivateAccessControl:
		return &response.AccessControl, nil
	case *schema.DeactivateAccessControlDeactivateAccessControlNotFoundError:
		return nil, types.NewErrNotFound(id, response.Typename, response.Message)
	case *schema.DeactivateAccessControlDeactivateAccessControlPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("deactivateAccessControl", response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", response)
	}
}

// GetAccessControl returns a specific AccessControl
func (a *AccessControlClient) GetAccessControl(ctx context.Context, id string) (*types.AccessControl, error) {
	result, err := schema.GetAccessControl(ctx, a.client, id)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.AccessControl.(type) {
	case *schema.GetAccessControlAccessControl:
		return &response.AccessControl, nil
	case *schema.GetAccessControlAccessControlNotFoundError:
		return nil, types.NewErrNotFound(id, response.Typename, response.Message)
	case *schema.GetAccessControlAccessControlPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("getAccessControl", response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", response)
	}
}

type AccessControlListOptions struct {
	order    []types.AccessControlOrderByInput
	filter   *types.AccessControlFilterInput
	cursor   *string
	pageSize int
}

// WithAccessControlListOrder can be used to specify the order of the returned AccessControl.
func WithAccessControlListOrder(input ...types.AccessControlOrderByInput) func(options *AccessControlListOptions) {
	return func(options *AccessControlListOptions) {
		options.order = append(options.order, input...)
	}
}

// WithAccessControlListFilter can be used to filter the returned AccessControls.
func WithAccessControlListFilter(input *types.AccessControlFilterInput) func(options *AccessControlListOptions) {
	return func(options *AccessControlListOptions) {
		options.filter = input
	}
}

// WithAccessControlListCursor sets the cursor to start listing from (for cursor-based pagination).
func WithAccessControlListCursor(cursor string) func(options *AccessControlListOptions) {
	return func(options *AccessControlListOptions) {
		options.cursor = &cursor
	}
}

// WithAccessControlListPageSize sets the number of items to return per page (default: internal.MaxPageSize).
// Returns ErrInvalidInput if size exceeds internal.MaxPageSize.
func WithAccessControlListPageSize(size int) func(options *AccessControlListOptions) {
	return func(options *AccessControlListOptions) {
		options.pageSize = size
	}
}

// ListAccessControls returns a list of AccessControls.
// The order of the list can be specified with WithAccessControlListOrder.
// A filter can be specified with WithAccessControlListFilter.
// A channel is returned that can be used to receive the list of AccessControls.
// To close the channel ensure to cancel the context.
func (a *AccessControlClient) ListAccessControls(ctx context.Context, ops ...func(*AccessControlListOptions)) iter.Seq2[*types.AccessControl, error] {
	options := AccessControlListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*schema.PageInfo, []schema.AccessControlConnectionEdgesAccessControlEdge, error) {
		output, err := schema.ListAccessControls(ctx, a.client, cursor, new(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch page := output.AccessControls.(type) {
		case *schema.ListAccessControlsAccessControlsAccessControlConnection:
			return &page.PageInfo.PageInfo, page.Edges, nil
		case *schema.ListAccessControlsAccessControlsPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("listAccessControls", page.Message)
		default:
			return nil, nil, errors.New("unreachable")
		}
	}

	edgeFn := func(edge *schema.AccessControlConnectionEdgesAccessControlEdge) (*string, *schema.AccessControl, error) {
		cursor := edge.Cursor
		if edge.Node == nil {
			return cursor, nil, nil
		}
		return cursor, &edge.Node.AccessControl, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, edgeFn)
}

// ListAccessControlsPage returns a single page of AccessControls.
// Use WithAccessControlListCursor to continue from a previous page.
// Use WithAccessControlListPageSize to override the default page size.
// The returned cursor is non-nil when more pages are available and should be passed
// to the next call via WithAccessControlListCursor.
func (a *AccessControlClient) ListAccessControlsPage(ctx context.Context, ops ...func(*AccessControlListOptions)) ([]*types.AccessControl, *string, error) {
	options := AccessControlListOptions{pageSize: internal.MaxPageSize}
	for _, op := range ops {
		op(&options)
	}

	if options.pageSize > internal.MaxPageSize {
		return nil, nil, types.NewErrInvalidInput(fmt.Sprintf("page size %d exceeds maximum of %d", options.pageSize, internal.MaxPageSize))
	}

	pageSize := options.pageSize

	output, err := schema.ListAccessControls(ctx, a.client, options.cursor, &pageSize, options.filter, options.order)
	if err != nil {
		return nil, nil, types.NewErrClient(err)
	}

	switch page := output.AccessControls.(type) {
	case *schema.ListAccessControlsAccessControlsAccessControlConnection:
		items := make([]*types.AccessControl, 0, len(page.Edges))
		var lastCursor *string

		for i := range page.Edges {
			edge := &page.Edges[i]
			if edge.Cursor != nil {
				lastCursor = edge.Cursor
			}

			if edge.Node != nil {
				ac := edge.Node.AccessControl
				items = append(items, &ac)
			}
		}

		var nextCursor *string
		if page.PageInfo.HasNextPage != nil && *page.PageInfo.HasNextPage {
			nextCursor = lastCursor
		}

		return items, nextCursor, nil
	case *schema.ListAccessControlsAccessControlsPermissionDeniedError:
		return nil, nil, types.NewErrPermissionDenied("listAccessControls", page.Message)
	default:
		return nil, nil, errors.New("unreachable")
	}
}

type AccessControlWhoListOptions struct {
	order []types.AccessControlWhoOrderByInput
}

// WithAccessControlWhoListOrder can be used to specify the order of the returned AccessControlWhoList
func WithAccessControlWhoListOrder(input ...schema.AccessControlWhoOrderByInput) func(options *AccessControlWhoListOptions) {
	return func(options *AccessControlWhoListOptions) {
		options.order = append(options.order, input...)
	}
}

// GetAccessControlWhoList returns all who items of an AccessControl.
// The order of the list can be specified with WithAccessControlWhoListOrder.
// A channel is returned that can be used to receive the list of AccessWhoItem.
// To close the channel ensure to cancel the context.
func (a *AccessControlClient) GetAccessControlWhoList(ctx context.Context, id string, ops ...func(*AccessControlWhoListOptions)) iter.Seq2[*types.AccessWhoItem, error] {
	options := AccessControlWhoListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.AccessWhoItemConnectionEdgesAccessWhoItemEdge, error) {
		output, err := schema.GetAccessControlWhoList(ctx, a.client, id, cursor, new(internal.MaxPageSize), nil, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch ap := output.AccessControl.(type) {
		case *schema.GetAccessControlWhoListAccessControl:
			switch whoList := ap.WhoList.(type) {
			case *schema.GetAccessControlWhoListAccessControlWhoListAccessWhoItemConnection:
				return &whoList.PageInfo.PageInfo, whoList.Edges, nil
			case *schema.GetAccessControlWhoListAccessControlWhoListPermissionDeniedError:
				return nil, nil, types.NewErrPermissionDenied("getAccessControlWhoList", whoList.Message)
			}
		case *schema.GetAccessControlWhoListAccessControlNotFoundError:
			return nil, nil, types.NewErrNotFound(id, ap.Typename, ap.Message)
		case *schema.GetAccessControlWhoListAccessControlPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("getAccessControlWhoList", ap.Message)
		default:
			return nil, nil, fmt.Errorf("unexpected type '%T': %w", ap, types.ErrUnknownType)
		}

		return nil, nil, errors.New("unreachable")
	}

	edgeFn := func(edge *types.AccessWhoItemConnectionEdgesAccessWhoItemEdge) (*string, *schema.AccessWhoItem, error) {
		cursor := edge.Cursor
		if edge.Node == nil {
			return cursor, nil, nil
		}
		return cursor, &edge.Node.AccessWhoItem, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, edgeFn)
}

type AccessControlWhatListOptions struct {
	order  []types.AccessWhatOrderByInput
	filter *types.AccessWhatFilterInput
}

// WithAccessControlWhatListOrder can be used to specify the order of the returned AccessControlWhatList
func WithAccessControlWhatListOrder(input ...types.AccessWhatOrderByInput) func(options *AccessControlWhatListOptions) {
	return func(options *AccessControlWhatListOptions) {
		options.order = append(options.order, input...)
	}
}

// WithAccessControlWhatListFilter can be used to filter the returned AccessControlWhatList.
func WithAccessControlWhatListFilter(input *types.AccessWhatFilterInput) func(options *AccessControlWhatListOptions) {
	return func(options *AccessControlWhatListOptions) {
		options.filter = input
	}
}

// GetAccessControlWhatDataObjectList returns all what items of an AccessControl.
// The order of the list can be specified with WithAccessControlWhatListOrder.
// A channel is returned that can be used to receive the list of AccessWhatDataObjectItem.
// To close the channel ensure to cancel the context.
func (a *AccessControlClient) GetAccessControlWhatDataObjectList(ctx context.Context, id string, ops ...func(*AccessControlWhatListOptions)) iter.Seq2[*types.AccessWhatDataObjectItem, error] {
	options := AccessControlWhatListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.AccessWhatDataObjectItemConnectionEdgesAccessWhatDataObjectItemEdge, error) {
		output, err := schema.GetAccessControlWhatDataObjectList(ctx, a.client, id, cursor, new(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch ap := output.AccessControl.(type) {
		case *schema.GetAccessControlWhatDataObjectListAccessControl:
			switch whatList := ap.WhatDataObjects.(type) {
			case *schema.GetAccessControlWhatDataObjectListAccessControlWhatDataObjectsAccessWhatDataObjectItemConnection:
				return &whatList.PageInfo.PageInfo, whatList.Edges, nil
			case *schema.GetAccessControlWhatDataObjectListAccessControlWhatDataObjectsPermissionDeniedError:
				return nil, nil, types.NewErrPermissionDenied("getAccessControlWhatDataObjectList", whatList.Message)
			}
		case *schema.GetAccessControlWhatDataObjectListAccessControlNotFoundError:
			return nil, nil, types.NewErrNotFound(id, ap.Typename, ap.Message)
		case *schema.GetAccessControlWhatDataObjectListAccessControlPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("getAccessControlWhatDataObjectList", ap.Message)
		default:
			return nil, nil, fmt.Errorf("unexpected type '%T': %w", ap, types.ErrUnknownType)
		}

		return nil, nil, errors.New("unreachable")
	}

	edgeFn := func(edge *types.AccessWhatDataObjectItemConnectionEdgesAccessWhatDataObjectItemEdge) (*string, *schema.AccessWhatDataObjectItem, error) {
		cursor := edge.Cursor
		if edge.Node == nil {
			return cursor, nil, nil
		}
		return cursor, &edge.Node.AccessWhatDataObjectItem, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, edgeFn)
}

// AccessControlWhatAccessControlListOptions options for listing what access controls of an AccessControl.
type AccessControlWhatAccessControlListOptions struct {
	order  []types.AccessWhatOrderByInput
	filter *types.AccessControlWhatAccessControlFilterInput
}

// WithAccessControlWhatAccessControlListOrder can be used to specify the order of the returned AccessControlWhatAccessControlList
func WithAccessControlWhatAccessControlListOrder(input ...schema.AccessWhatOrderByInput) func(options *AccessControlWhatAccessControlListOptions) {
	return func(options *AccessControlWhatAccessControlListOptions) {
		options.order = append(options.order, input...)
	}
}

// WithAccessControlWhatAccessControlListFilter can be used to specify the filter of the returned AccessControlWhatAccessControlList.
func WithAccessControlWhatAccessControlListFilter(filter *types.AccessControlWhatAccessControlFilterInput) func(options *AccessControlWhatAccessControlListOptions) {
	return func(options *AccessControlWhatAccessControlListOptions) {
		options.filter = filter
	}
}

// GetAccessControlWhatAccessControlList returns all what access controls of an AccessControl.
func (a *AccessControlClient) GetAccessControlWhatAccessControlList(ctx context.Context, id string, ops ...func(*AccessControlWhatAccessControlListOptions)) iter.Seq2[*types.AccessWhatAccessControlItem, error] {
	options := AccessControlWhatAccessControlListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.AccessWhatAccessControlItemConnectionEdgesAccessWhatAccessControlItemEdge, error) {
		output, err := schema.GetAccessControlWhatAccessControls(ctx, a.client, id, cursor, new(internal.MaxPageSize), options.order, options.filter)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch ap := output.AccessControl.(type) {
		case *schema.GetAccessControlWhatAccessControlsAccessControl:
			switch whatList := ap.WhatAccessControls.(type) {
			case *schema.GetAccessControlWhatAccessControlsAccessControlWhatAccessControlsAccessWhatAccessControlItemConnection:
				return &whatList.PageInfo.PageInfo, whatList.Edges, nil
			case *schema.GetAccessControlWhatAccessControlsAccessControlWhatAccessControlsPermissionDeniedError:
				return nil, nil, types.NewErrPermissionDenied("getAccessControlWhatAccessControls", whatList.Message)
			case *schema.GetAccessControlWhatAccessControlsAccessControlWhatAccessControlsInvalidInputError:
				return nil, nil, types.NewErrInvalidInput(whatList.Message)
			case *schema.GetAccessControlWhatAccessControlsAccessControlWhatAccessControlsNotFoundError:
				return nil, nil, types.NewErrNotFound("", whatList.Typename, whatList.Message)
			default:
				return nil, nil, fmt.Errorf("unexpected type '%T': %w", whatList, types.ErrUnknownType)
			}
		case *schema.GetAccessControlWhatAccessControlsAccessControlNotFoundError:
			return nil, nil, types.NewErrNotFound(id, ap.Typename, ap.Message)
		case *schema.GetAccessControlWhatAccessControlsAccessControlPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("accessControl", ap.Message)
		default:
			return nil, nil, fmt.Errorf("unexpected type '%T': %w", ap, types.ErrUnknownType)
		}
	}

	edgeFn := func(edge *types.AccessWhatAccessControlItemConnectionEdgesAccessWhatAccessControlItemEdge) (*string, *types.AccessWhatAccessControlItem, error) {
		cursor := edge.Cursor
		if edge.Node == nil {
			return cursor, nil, nil
		}
		return cursor, &edge.Node.AccessWhatAccessControlItem, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, edgeFn)
}

type AccessControlAbacWhatScopeListOptions struct {
	order  []types.AccessWhatOrderByInput
	search *string
}

// WithAccessControlAbacWhatScopeListOrder can be used to specify the order of the returned AccessControlAbacWhatScopeList.
func WithAccessControlAbacWhatScopeListOrder(input ...types.AccessWhatOrderByInput) func(options *AccessControlAbacWhatScopeListOptions) {
	return func(options *AccessControlAbacWhatScopeListOptions) {
		options.order = append(options.order, input...)
	}
}

// WithAccessControlAbacWhatScopeListSearch can be used to specify the search of the returned Access
func WithAccessControlAbacWhatScopeListSearch(search string) func(options *AccessControlAbacWhatScopeListOptions) {
	return func(options *AccessControlAbacWhatScopeListOptions) {
		options.search = &search
	}
}

// GetAccessControlAbacWhatScope returns all abac what scopes of an AccessControl
// id is the id of the AccessControl
// WithAccessControlAbacWhatScopeListSearch can be used to specify the search of the returned types.DataObject
// WithAccessControlAbacWhatScopeListOrder can be used to specify the order of the returned types.DataObject
func (a *AccessControlClient) GetAccessControlAbacWhatScope(ctx context.Context, id string, abacRule string, ops ...func(*AccessControlAbacWhatScopeListOptions)) iter.Seq2[*types.DataObject, error] {
	options := AccessControlAbacWhatScopeListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.DataObjectConnectionEdgesDataObjectEdge, error) {
		output, err := schema.ListAccessControlAbacWhatScope(ctx, a.client, id, cursor, new(internal.MaxPageSize), options.search, abacRule, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch ap := output.AccessControl.(type) {
		case *schema.ListAccessControlAbacWhatScopeAccessControl:
			switch whatList := ap.WhatAbacScope.(type) {
			case *schema.ListAccessControlAbacWhatScopeAccessControlWhatAbacScopeDataObjectConnection:
				return &whatList.PageInfo.PageInfo, whatList.Edges, nil
			case *schema.ListAccessControlAbacWhatScopeAccessControlWhatAbacScopePermissionDeniedError:
				return nil, nil, types.NewErrPermissionDenied("accessControlWhatAbacScopeList", whatList.Message)
			default:
				return nil, nil, fmt.Errorf("unexpected type '%T': %w", whatList, types.ErrUnknownType)
			}
		case *schema.ListAccessControlAbacWhatScopeAccessControlPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("accessControl", ap.Message)
		case *schema.ListAccessControlAbacWhatScopeAccessControlNotFoundError:
			return nil, nil, types.NewErrNotFound(id, ap.Typename, ap.Message)
		default:
			return nil, nil, fmt.Errorf("unexpected type '%T': %w", ap, types.ErrUnknownType)
		}
	}

	edgeFn := func(edge *types.DataObjectConnectionEdgesDataObjectEdge) (*string, *types.DataObject, error) {
		cursor := edge.Cursor
		if edge.Node == nil {
			return cursor, nil, nil
		}
		return cursor, &edge.Node.DataObject, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, edgeFn)
}
