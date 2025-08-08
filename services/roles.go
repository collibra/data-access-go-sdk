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

type RoleClient struct {
	client graphql.Client
}

func NewRoleClient(client graphql.Client) RoleClient {
	return RoleClient{
		client: client,
	}
}

// GetRole returns a role by ID
// Returns a Role if role is retrieved successfully, otherwise returns an error.
func (c *RoleClient) GetRole(ctx context.Context, id string) (*types.Role, error) {
	result, err := schema.GetRole(ctx, c.client, id)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	return &result.Role.Role, nil
}

type RoleListOptions struct {
	order  []types.RoleOrderByInput
	filter *types.RoleFilterInput
}

// WithRoleListOrder sets the order of the returned roles
func WithRoleListOrder(input ...types.RoleOrderByInput) func(options *RoleListOptions) {
	return func(options *RoleListOptions) {
		options.order = append(options.order, input...)
	}
}

// WithRoleListFilter sets the filter of the returned roles
func WithRoleListFilter(input *types.RoleFilterInput) func(options *RoleListOptions) {
	return func(options *RoleListOptions) {
		options.filter = input
	}
}

// ListRoles returns a list of roles
// The order of the list can be specified with WithRoleListOrder.
// A filter can be specified with WithRoleListFilter.
// A channel is returned that can be used to receive the list of types.Role.
// To close the channel ensure to cancel the context.
func (c *RoleClient) ListRoles(ctx context.Context, ops ...func(*RoleListOptions)) <-chan types.ListItem[types.Role] { //nolint:dupl
	options := RoleListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.RolePageEdgesEdge, error) {
		output, err := schema.ListRoles(ctx, c.client, cursor, ptr.Int(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		return &output.Roles.PageInfo.PageInfo, output.Roles.Edges, nil
	}

	edgeFn := func(edge *types.RolePageEdgesEdge) (*string, *schema.Role, error) {
		cursor := edge.Cursor

		if edge.Node == nil {
			return cursor, nil, nil
		}

		listItem := (*edge.Node).(*types.RolePageEdgesEdgeNodeRole)

		return cursor, &listItem.Role, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, edgeFn)
}

type RoleAssignmentListOptions struct {
	order  []types.RoleAssignmentOrderInput
	filter *types.RoleAssignmentFilterInput
}

// WithRoleAssignmentListOrder sets the order of the returned role assignments
func WithRoleAssignmentListOrder(input ...types.RoleAssignmentOrderInput) func(options *RoleAssignmentListOptions) {
	return func(options *RoleAssignmentListOptions) {
		options.order = append(options.order, input...)
	}
}

// WithRoleAssignmentListFilter sets the filter of the returned role assignments
func WithRoleAssignmentListFilter(input *types.RoleAssignmentFilterInput) func(options *RoleAssignmentListOptions) {
	return func(options *RoleAssignmentListOptions) {
		options.filter = input
	}
}

// ListRoleAssignments returns a list of role assignments for a given role
// The order of the list can be specified with WithRoleAssignmentListOrder.
// A filter can be specified with WithRoleAssignmentListFilter
// A channel is returned that can be used to receive the list of types.RoleAssignment
// To close the channel ensure to cancel the context.
func (c *RoleClient) ListRoleAssignments(ctx context.Context, ops ...func(*RoleAssignmentListOptions)) <-chan types.ListItem[types.RoleAssignment] {
	options := RoleAssignmentListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.RoleAssignmentPageEdgesEdge, error) {
		output, err := schema.ListRoleAssignments(ctx, c.client, cursor, ptr.Int(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		return &output.RoleAssignments.PageInfo.PageInfo, output.RoleAssignments.Edges, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, roleAssignmentsEdgeFn)
}

// ListRoleAssignmentsOnIdentityStore returns a list of role assignments for a given role on a given identity
// The order of the list can be specified with WithRoleAssignmentListOrder.
// A filter can be specified with WithRoleAssignmentListFilter.
// A channel is returned that can be used to receive the list of types.RoleAssignment.
// To close the channel ensure to cancel the context.
func (c *RoleClient) ListRoleAssignmentsOnIdentityStore(ctx context.Context, identityId string, ops ...func(*RoleAssignmentListOptions)) <-chan types.ListItem[types.RoleAssignment] {
	options := RoleAssignmentListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.RoleAssignmentPageEdgesEdge, error) {
		output, err := schema.ListRoleAssignmentsOnIdentityStore(ctx, c.client, identityId, cursor, ptr.Int(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch is := output.IdentityStore.(type) {
		case *schema.ListRoleAssignmentsOnIdentityStoreIdentityStore:
			return &is.RoleAssignments.PageInfo.PageInfo, is.RoleAssignments.Edges, nil
		case *schema.ListRoleAssignmentsOnIdentityStoreIdentityStoreAlreadyExistsError:
			return nil, nil, types.NewErrAlreadyExists("listRoleAssignmentsOnIdentityStore", is.Message)
		case *schema.ListRoleAssignmentsOnIdentityStoreIdentityStoreNotFoundError:
			return nil, nil, types.NewErrNotFound(identityId, is.Typename, is.Message)
		case *schema.ListRoleAssignmentsOnIdentityStoreIdentityStorePermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("listRoleAssignmentsOnIdentityStore", is.Message)
		default:
			return nil, nil, fmt.Errorf("unexpected type '%T'", is)
		}
	}

	return internal.PaginationExecutor(ctx, loadPageFn, roleAssignmentsEdgeFn)
}

// ListRoleAssignmentsOnDataObject returns a list of role assignments for a given role on a given data object
// The order of the list can be specified with WithRoleAssignmentListOrder.
// A filter can be specified with WithRoleAssignmentListFilter.
// A channel is returned that can be used to receive the list of types.RoleAssignment.
// To close the channel ensure to cancel the context.
func (c *RoleClient) ListRoleAssignmentsOnDataObject(ctx context.Context, objectId string, ops ...func(*RoleAssignmentListOptions)) <-chan types.ListItem[types.RoleAssignment] {
	options := RoleAssignmentListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.RoleAssignmentPageEdgesEdge, error) {
		output, err := schema.ListRoleAssignmentsOnDataObject(ctx, c.client, objectId, cursor, ptr.Int(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		return &output.DataObject.RoleAssignments.PageInfo.PageInfo, output.DataObject.RoleAssignments.Edges, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, roleAssignmentsEdgeFn)
}

// ListRoleAssignmentsOnDataSource returns a list of role assignments for a given role on a given data source
// The order of the list can be specified with WithRoleAssignmentListOrder.
// A filter can be specified with WithRoleAssignmentListFilter.
// A channel is returned that can be used to receive the list of types.RoleAssignment.
// To close the channel ensure to cancel the context.
func (c *RoleClient) ListRoleAssignmentsOnDataSource(ctx context.Context, dataSourceId string, ops ...func(*RoleAssignmentListOptions)) <-chan types.ListItem[types.RoleAssignment] { //nolint:dupl
	options := RoleAssignmentListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.RoleAssignmentPageEdgesEdge, error) {
		output, err := schema.ListRoleAssignmentsOnDataSource(ctx, c.client, dataSourceId, cursor, ptr.Int(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch ds := output.DataSource.(type) {
		case *schema.ListRoleAssignmentsOnDataSourceDataSource:
			return &ds.RoleAssignments.PageInfo.PageInfo, ds.RoleAssignments.Edges, nil
		case *schema.ListRoleAssignmentsOnDataSourceDataSourcePermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("listRoleAssignmentsOnDataSource", ds.Message)
		case *schema.ListRoleAssignmentsOnDataSourceDataSourceNotFoundError:
			return nil, nil, types.NewErrNotFound(dataSourceId, ds.Typename, ds.Message)
		default:
			return nil, nil, fmt.Errorf("unexpected type '%T'", ds)
		}
	}

	return internal.PaginationExecutor(ctx, loadPageFn, roleAssignmentsEdgeFn)
}

// ListRoleAssignmentsOnAccessProvider returns a list of role assignments for a given role on an access provider.
// The order of the list can be specified with WithRoleAssignmentListOrder.
// A filter can be specified with WithRoleAssignmentListFilter.
// A channel is returned that can be used to receive the list of types.RoleAssignment.
// To close the channel ensure to cancel the context.
func (c *RoleClient) ListRoleAssignmentsOnAccessProvider(ctx context.Context, accessProviderId string, ops ...func(*RoleAssignmentListOptions)) <-chan types.ListItem[types.RoleAssignment] { //nolint:dupl
	options := RoleAssignmentListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.RoleAssignmentPageEdgesEdge, error) {
		output, err := schema.ListRoleAssignmentsOnAccessProvider(ctx, c.client, accessProviderId, cursor, ptr.Int(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch ap := output.AccessProvider.(type) {
		case *schema.ListRoleAssignmentsOnAccessProviderAccessProvider:
			return &ap.RoleAssignments.PageInfo.PageInfo, ap.RoleAssignments.Edges, nil
		case *schema.ListRoleAssignmentsOnAccessProviderAccessProviderPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("listRoleAssignmentsOnAccessProvider", ap.Message)
		case *schema.ListRoleAssignmentsOnAccessProviderAccessProviderNotFoundError:
			return nil, nil, types.NewErrNotFound(accessProviderId, ap.Typename, ap.Message)
		default:
			return nil, nil, fmt.Errorf("unexpected type '%T'", ap)
		}
	}

	return internal.PaginationExecutor(ctx, loadPageFn, roleAssignmentsEdgeFn)
}

// ListRoleAssignmentsOnUser returns a list of role assignments for a given role on a given user.
// The order of the list can be specified with WithRoleAssignmentListOrder.
// A filter can be specified with WithRoleAssignmentListFilter.
// A channel is returned that can be used to receive the list of types.RoleAssignment.
// To close the channel ensure to cancel the context.
func (c *RoleClient) ListRoleAssignmentsOnUser(ctx context.Context, userId string, ops ...func(*RoleAssignmentListOptions)) <-chan types.ListItem[types.RoleAssignment] {
	options := RoleAssignmentListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.RoleAssignmentPageEdgesEdge, error) {
		output, err := schema.ListRoleAssignmentsOnUser(ctx, c.client, userId, cursor, ptr.Int(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch r := output.User.(type) {
		case *schema.ListRoleAssignmentsOnUserUser:
			return &r.RoleAssignments.PageInfo.PageInfo, r.RoleAssignments.Edges, nil
		case *schema.ListRoleAssignmentsOnUserUserPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("listRoleAssignmentsOnUser", r.Message)
		case *schema.ListRoleAssignmentsOnUserUserNotFoundError:
			return nil, nil, types.NewErrNotFound(userId, r.Typename, r.Message)
		case *schema.ListRoleAssignmentsOnUserUserInvalidEmailError:
			return nil, nil, types.NewErrInvalidEmail(userId, r.Message)
		case *schema.ListRoleAssignmentsOnUserUserInvalidInputError:
			return nil, nil, types.NewErrInvalidInput(r.Message)
		default:
			return nil, nil, fmt.Errorf("unexpected type '%T'", r)
		}
	}

	return internal.PaginationExecutor(ctx, loadPageFn, roleAssignmentsEdgeFn)
}

// AssignRoleOnIdentityStore create a role assignment between an IdentityStore and a set of users.
// roleId is the id of the role to assign.
// isId is the id of the identity store to assign the role to.
// to is a list of user ids to assign the role to.
func (c *RoleClient) AssignRoleOnIdentityStore(ctx context.Context, roleId string, isId string, to ...string) (*types.Role, error) {
	output, err := schema.AssignRoleOnIdentityStore(ctx, c.client, roleId, isId, to)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch r := output.AssignRoleOnIdentityStore.(type) {
	case *schema.AssignRoleOnIdentityStoreAssignRoleOnIdentityStoreRole:
		return &r.Role, nil
	case *schema.AssignRoleOnIdentityStoreAssignRoleOnIdentityStorePermissionDeniedError:
		return nil, types.NewErrPermissionDenied("assignRoleOnIdentityStore", r.Message)
	case *schema.AssignRoleOnIdentityStoreAssignRoleOnIdentityStoreNotFoundError:
		return nil, types.NewErrNotFound(isId, r.Typename, r.Message)
	default:
		return nil, fmt.Errorf("unexpected type '%T'", r)
	}
}

// AssignRoleOnDataObject create a role assignment between a data object and a set of users.
// roleId is the id of the role to assign.
// isId is the id of the identity store to assign the role to.
// to is a list of user ids to assign the role to.
func (c *RoleClient) AssignRoleOnDataObject(ctx context.Context, roleId string, doId string, to ...string) (*types.Role, error) {
	output, err := schema.AssignRoleOnDataObject(ctx, c.client, roleId, doId, to)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch r := output.AssignRoleOnDataObject.(type) {
	case *schema.AssignRoleOnDataObjectAssignRoleOnDataObjectRole:
		return &r.Role, nil
	case *schema.AssignRoleOnDataObjectAssignRoleOnDataObjectPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("assignRoleOnDataObject", r.Message)
	case *schema.AssignRoleOnDataObjectAssignRoleOnDataObjectNotFoundError:
		return nil, types.NewErrNotFound(doId, r.Typename, r.Message)
	default:
		return nil, fmt.Errorf("unexpected type '%T'", r)
	}
}

// AssignRoleOnDataSource create a role assignment between a data source and a set of users.
// roleId is the id of the role to assign.
// dataSourceId is the id of the data source to assign the role to.
// to is a list of user ids to assign the role to.
func (c *RoleClient) AssignRoleOnDataSource(ctx context.Context, roleId string, dataSourceId string, to ...string) (*types.Role, error) {
	output, err := schema.AssignRoleOnDataSource(ctx, c.client, roleId, dataSourceId, to)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch r := output.AssignRoleOnDataSource.(type) {
	case *schema.AssignRoleOnDataSourceAssignRoleOnDataSourceRole:
		return &r.Role, nil
	case *schema.AssignRoleOnDataSourceAssignRoleOnDataSourcePermissionDeniedError:
		return nil, types.NewErrPermissionDenied("assignRoleOnDataSource", r.Message)
	case *schema.AssignRoleOnDataSourceAssignRoleOnDataSourceNotFoundError:
		return nil, types.NewErrNotFound(dataSourceId, r.Typename, r.Message)
	default:
		return nil, fmt.Errorf("unexpected type '%T'", r)
	}
}

// AssignRoleOnAccessProvider create a role assignment between an access provider and a set of users.
// roleId is the id of the role to assign.
// accessProviderId is the id of the access provider to assign the role to.
// to is a list of user ids to assign the role to.
func (c *RoleClient) AssignRoleOnAccessProvider(ctx context.Context, roleId string, accessProviderId string, to ...string) (*types.Role, error) {
	output, err := schema.AssignRoleOnAccessProvider(ctx, c.client, roleId, accessProviderId, to)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch r := output.AssignRoleOnAccessProvider.(type) {
	case *schema.AssignRoleOnAccessProviderAssignRoleOnAccessProviderRole:
		return &r.Role, nil
	case *schema.AssignRoleOnAccessProviderAssignRoleOnAccessProviderPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("assignRoleOnAccessProvider", r.Message)
	case *schema.AssignRoleOnAccessProviderAssignRoleOnAccessProviderNotFoundError:
		return nil, types.NewErrNotFound(accessProviderId, r.Typename, r.Message)
	default:
		return nil, fmt.Errorf("unexpected type '%T'", r)
	}
}

// AssignGlobalRole create a role assignment between a global role and a set of users.
// roleId is the id of the role to assign.
// to is a list of user ids to assign the role to.
func (c *RoleClient) AssignGlobalRole(ctx context.Context, roelId string, to ...string) (*types.Role, error) {
	output, err := schema.AssignGlobalRole(ctx, c.client, roelId, to)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch r := output.AssignGlobalRole.(type) {
	case *schema.AssignGlobalRoleAssignGlobalRole:
		return &r.Role, nil
	case *schema.AssignGlobalRoleAssignGlobalRolePermissionDeniedError:
		return nil, types.NewErrPermissionDenied("assignGlobalRole", r.Message)
	case *schema.AssignGlobalRoleAssignGlobalRoleNotFoundError:
		return nil, types.NewErrNotFound(roelId, r.Typename, r.Message)
	default:
		return nil, fmt.Errorf("unexpected type '%T'", r)
	}
}

// UnassignRoleFromIdentityStore removes a role assignment between an IdentityStore and a set of users
// roleId is the id of the role to unassign.
// isId is the id of the identity store to unassign the role from.
// from is a list of user ids to unassign the role from.
func (c *RoleClient) UnassignRoleFromIdentityStore(ctx context.Context, roleId string, isId string, from ...string) (*types.Role, error) {
	output, err := schema.UnassignRoleFromIdentityStore(ctx, c.client, roleId, isId, from)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch r := output.UnassignRoleFromIdentityStore.(type) {
	case *schema.UnassignRoleFromIdentityStoreUnassignRoleFromIdentityStoreRole:
		return &r.Role, nil
	case *schema.UnassignRoleFromIdentityStoreUnassignRoleFromIdentityStorePermissionDeniedError:
		return nil, types.NewErrPermissionDenied("unassignRoleFromIdentityStore", r.Message)
	case *schema.UnassignRoleFromIdentityStoreUnassignRoleFromIdentityStoreNotFoundError:
		return nil, types.NewErrNotFound(isId, r.Typename, r.Message)
	default:
		return nil, fmt.Errorf("unexpected type '%T'", r)
	}
}

// UnassignRoleFromDataObject removes a role assignment between a data object and a set of users.
// roleId is the id of the role to unassign.
// doId is the id of the data object to unassign the role from.
// from is a list of user ids to unassign the role from.
func (c *RoleClient) UnassignRoleFromDataObject(ctx context.Context, roleId string, doId string, from ...string) (*types.Role, error) {
	output, err := schema.UnassignRoleFromDataObject(ctx, c.client, roleId, doId, from)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch r := output.UnassignRoleFromDataObject.(type) {
	case *schema.UnassignRoleFromDataObjectUnassignRoleFromDataObjectRole:
		return &r.Role, nil
	case *schema.UnassignRoleFromDataObjectUnassignRoleFromDataObjectPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("unassignRoleFromDataObject", r.Message)
	case *schema.UnassignRoleFromDataObjectUnassignRoleFromDataObjectNotFoundError:
		return nil, types.NewErrNotFound(doId, r.Typename, r.Message)
	default:
		return nil, fmt.Errorf("unexpected type '%T'", r)
	}
}

// UnassignRoleFromDataSource removes a role assignment between a data source and a set of users.
// roleId is the id of the role to unassign.
// dataSourceId is the id of the data source to unassign the role from.
// from is a list of user ids to unassign the role from.
func (c *RoleClient) UnassignRoleFromDataSource(ctx context.Context, roleId string, dataSourceId string, from ...string) (*types.Role, error) {
	output, err := schema.UnassignRoleFromDataSource(ctx, c.client, roleId, dataSourceId, from)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch r := output.UnassignRoleFromDataSource.(type) {
	case *schema.UnassignRoleFromDataSourceUnassignRoleFromDataSourceRole:
		return &r.Role, nil
	case *schema.UnassignRoleFromDataSourceUnassignRoleFromDataSourcePermissionDeniedError:
		return nil, types.NewErrPermissionDenied("unassignRoleFromDataSource", r.Message)
	case *schema.UnassignRoleFromDataSourceUnassignRoleFromDataSourceNotFoundError:
		return nil, types.NewErrNotFound(dataSourceId, r.Typename, r.Message)
	default:
		return nil, fmt.Errorf("unexpected type '%T'", r)
	}
}

// UnassignRoleFromAccessProvider removes a role assignment between an access provider and a set of users
// roleId is the id of the role to unassign.
// accessProviderId is the id of the access provider to unassign the role from.
// from is a list of user ids to unassign the role from.
func (c *RoleClient) UnassignRoleFromAccessProvider(ctx context.Context, roleId string, accessProviderId string, from ...string) (*types.Role, error) {
	output, err := schema.UnassignRoleFromAccessProvider(ctx, c.client, roleId, accessProviderId, from)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	if output.UnassignRoleFromAccessProvider == nil {
		return nil, types.NewErrClient(errors.New("unknown error"))
	}

	switch r := (*output.UnassignRoleFromAccessProvider).(type) {
	case *schema.UnassignRoleFromAccessProviderUnassignRoleFromAccessProviderRole:
		return &r.Role, nil
	case *schema.UnassignRoleFromAccessProviderUnassignRoleFromAccessProviderPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("unassignRoleFromAccessProvider", r.Message)
	case *schema.UnassignRoleFromAccessProviderUnassignRoleFromAccessProviderNotFoundError:
		return nil, types.NewErrNotFound(accessProviderId, r.Typename, r.Message)
	default:
		return nil, fmt.Errorf("unexpected type '%T'", r)
	}
}

// UnassignGlobalRole removes a role assignment between a global role and a set of users.
// roleId is the id of the role to unassign.
// from is a list of user ids to unassign the role from.
func (c *RoleClient) UnassignGlobalRole(ctx context.Context, roleId string, from ...string) (*types.Role, error) {
	output, err := schema.UnassignGlobalRole(ctx, c.client, roleId, from)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch r := output.UnassignGlobalRole.(type) {
	case *schema.UnassignGlobalRoleUnassignGlobalRole:
		return &r.Role, nil
	case *schema.UnassignGlobalRoleUnassignGlobalRolePermissionDeniedError:
		return nil, types.NewErrPermissionDenied("unassignGlobalRole", r.Message)
	case *schema.UnassignGlobalRoleUnassignGlobalRoleNotFoundError:
		return nil, types.NewErrNotFound(roleId, r.Typename, r.Message)
	default:
		return nil, fmt.Errorf("unexpected type '%T'", r)
	}
}

// UpdateRoleAssigneesOnIdentityStore updates a role assignment between an IdentityStore and a set of users.
// Existing role assignments will be overwritten.
// isId is the id of the identity store to assign the role to.
// roleId is the id of the role to assign.
// assignees is a list of user ids to assign the role to.
func (c *RoleClient) UpdateRoleAssigneesOnIdentityStore(ctx context.Context, isId string, roleId string, assignees ...string) (*types.Role, error) {
	output, err := schema.UpdateRoleAssigneesOnIdentityStore(ctx, c.client, isId, roleId, assignees)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch r := output.UpdateRoleAssigneesOnIdentityStore.(type) {
	case *schema.UpdateRoleAssigneesOnIdentityStoreUpdateRoleAssigneesOnIdentityStoreRole:
		return &r.Role, nil
	case *schema.UpdateRoleAssigneesOnIdentityStoreUpdateRoleAssigneesOnIdentityStorePermissionDeniedError:
		return nil, types.NewErrPermissionDenied("updateRoleAssigneesOnIdentityStore", r.Message)
	case *schema.UpdateRoleAssigneesOnIdentityStoreUpdateRoleAssigneesOnIdentityStoreNotFoundError:
		return nil, types.NewErrNotFound(isId, r.Typename, r.Message)
	default:
		return nil, fmt.Errorf("unexpected type '%T'", r)
	}
}

// UpdateRoleAssigneesOnDataObject updates a role assignment between a data object and a set of users.
// Existing role assignments will be overwritten.
// doId is the id of the data object to assign the role to.
// roleId is the id of the role to assign.
// assignees is a list of user ids to assign the role to.
func (c *RoleClient) UpdateRoleAssigneesOnDataObject(ctx context.Context, doId string, roleId string, assignees ...string) (*types.Role, error) {
	output, err := schema.UpdateRoleAssigneesOnDataObject(ctx, c.client, doId, roleId, assignees)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch r := output.UpdateRoleAssigneesOnDataObject.(type) {
	case *schema.UpdateRoleAssigneesOnDataObjectUpdateRoleAssigneesOnDataObjectRole:
		return &r.Role, nil
	case *schema.UpdateRoleAssigneesOnDataObjectUpdateRoleAssigneesOnDataObjectPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("updateRoleAssigneesOnDataObject", r.Message)
	case *schema.UpdateRoleAssigneesOnDataObjectUpdateRoleAssigneesOnDataObjectNotFoundError:
		return nil, types.NewErrNotFound(doId, r.Typename, r.Message)
	default:
		return nil, fmt.Errorf("unexpected type '%T'", r)
	}
}

// UpdateRoleAssigneesOnDataSource updates a role assignment between a data source and a set of users
// Existing role assignments will be overwritten.
// dataSourceId is the id of the data source to assign the role to.
// roleId is the id of the role to assign.
// assignees is a list of user ids to assign the role to.
func (c *RoleClient) UpdateRoleAssigneesOnDataSource(ctx context.Context, dataSourceId string, roleId string, assignees ...string) (*types.Role, error) {
	output, err := schema.UpdateRoleAssigneesOnDataSource(ctx, c.client, dataSourceId, roleId, assignees)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch r := output.UpdateRoleAssigneesOnDataSource.(type) {
	case *schema.UpdateRoleAssigneesOnDataSourceUpdateRoleAssigneesOnDataSourceRole:
		return &r.Role, nil
	case *schema.UpdateRoleAssigneesOnDataSourceUpdateRoleAssigneesOnDataSourcePermissionDeniedError:
		return nil, types.NewErrPermissionDenied("updateRoleAssigneesOnDataSource", r.Message)
	case *schema.UpdateRoleAssigneesOnDataSourceUpdateRoleAssigneesOnDataSourceNotFoundError:
		return nil, types.NewErrNotFound(dataSourceId, r.Typename, r.Message)
	default:
		return nil, fmt.Errorf("unexpected type '%T'", r)
	}
}

// UpdateRoleAssigneesOnAccessProvider updates a role assignment between an access provider and a set of users.
// Existing role assignments will be overwritten.
// accessProviderId is the id of the access provider to assign the role to.
// roleId is the id of the role to assign.
// assignees is a list of user ids to assign the role to.
func (c *RoleClient) UpdateRoleAssigneesOnAccessProvider(ctx context.Context, accessProviderId string, roleId string, assignees ...string) (*types.Role, error) {
	output, err := schema.UpdateRoleAssigneesOnAccessProvider(ctx, c.client, accessProviderId, roleId, assignees)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch r := output.UpdateRoleAssigneesOnAccessProvider.(type) {
	case *schema.UpdateRoleAssigneesOnAccessProviderUpdateRoleAssigneesOnAccessProviderRole:
		return &r.Role, nil
	case *schema.UpdateRoleAssigneesOnAccessProviderUpdateRoleAssigneesOnAccessProviderPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("updateRoleAssigneesOnAccessProvider", r.Message)
	case *schema.UpdateRoleAssigneesOnAccessProviderUpdateRoleAssigneesOnAccessProviderNotFoundError:
		return nil, types.NewErrNotFound(accessProviderId, r.Typename, r.Message)
	default:
		return nil, fmt.Errorf("unexpected type '%T'", r)
	}
}

// SetGlobalRoleForUsers sets a global role for a set of users.
// Existing global role assignments will be overwritten.
// roleId is the id of the global role to assign.
// assignees is a list of user ids to assign the global role to.
func (c *RoleClient) SetGlobalRoleForUsers(ctx context.Context, roleId string, assignees ...string) error {
	output, err := schema.SetGlobalRolesForUser(ctx, c.client, roleId, assignees)
	if err != nil {
		return types.NewErrClient(err)
	}

	switch r := output.SetGlobalRolesForUser.(type) {
	case *schema.SetGlobalRolesForUserSetGlobalRolesForUser:
		if r.Success {
			return nil
		} else {
			return types.NewErrClient(errors.New("unknown server error"))
		}
	case *schema.SetGlobalRolesForUserSetGlobalRolesForUserPermissionDeniedError:
		return types.NewErrPermissionDenied("setGlobalRolesForUser", r.Message)
	default:
		return fmt.Errorf("unexpected type '%T'", r)
	}
}

func roleAssignmentsEdgeFn(edge *types.RoleAssignmentPageEdgesEdge) (*string, *schema.RoleAssignment, error) {
	cursor := edge.Cursor

	if edge.Node == nil {
		return cursor, nil, nil
	}

	listItem := (*edge.Node).(*types.RoleAssignmentPageEdgesEdgeNodeRoleAssignment)

	return cursor, &listItem.RoleAssignment, nil
}
