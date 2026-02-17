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

type RoleClient struct {
	client graphql.Client
}

func NewRoleClient(client graphql.Client) *RoleClient {
	return &RoleClient{
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
func (c *RoleClient) ListRoleAssignments(ctx context.Context, ops ...func(*RoleAssignmentListOptions)) iter.Seq2[*types.RoleAssignment, error] {
	options := RoleAssignmentListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.RoleAssignmentConnectionEdgesRoleAssignmentEdge, error) { //nolint:dupl
		output, err := schema.ListRoleAssignments(ctx, c.client, cursor, new(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch response := (output.RoleAssignments).(type) {
		case *schema.ListRoleAssignmentsRoleAssignmentsRoleAssignmentConnection:
			return &response.PageInfo.PageInfo, response.Edges, nil
		case *schema.ListRoleAssignmentsRoleAssignmentsInvalidInputError:
			return nil, nil, types.NewErrInvalidInput(response.Message)
		case *schema.ListRoleAssignmentsRoleAssignmentsNotFoundError:
			return nil, nil, types.NewErrNotFound("", response.Typename, response.Message)
		case *schema.ListRoleAssignmentsRoleAssignmentsPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("listRoleAssignments", response.Message)
		default:
			return nil, nil, fmt.Errorf("unexpected type '%T'", response)
		}
	}

	return internal.PaginationExecutor(ctx, loadPageFn, roleAssignmentsEdgeFn)
}

// ListRoleAssignmentsOnDataObject returns a list of role assignments for a given role on a given data object
// The order of the list can be specified with WithRoleAssignmentListOrder.
// A filter can be specified with WithRoleAssignmentListFilter.
// A channel is returned that can be used to receive the list of types.RoleAssignment.
// To close the channel ensure to cancel the context.
func (c *RoleClient) ListRoleAssignmentsOnDataObject(ctx context.Context, objectId string, ops ...func(*RoleAssignmentListOptions)) iter.Seq2[*types.RoleAssignment, error] {
	options := RoleAssignmentListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.RoleAssignmentConnectionEdgesRoleAssignmentEdge, error) {
		output, err := schema.ListRoleAssignmentsOnDataObject(ctx, c.client, objectId, cursor, new(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch result := (output.DataObject.RoleAssignments).(type) {
		case *schema.ListRoleAssignmentsOnDataObjectDataObjectRoleAssignmentsRoleAssignmentConnection:
			return &result.PageInfo.PageInfo, result.Edges, nil
		case *schema.ListRoleAssignmentsOnDataObjectDataObjectRoleAssignmentsNotFoundError:
			return nil, nil, types.NewErrNotFound(objectId, result.Typename, result.Message)
		case *schema.ListRoleAssignmentsOnDataObjectDataObjectRoleAssignmentsPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("listRoleAssignmentsOnDataObject", result.Message)
		case *schema.ListRoleAssignmentsOnDataObjectDataObjectRoleAssignmentsInvalidInputError:
			return nil, nil, types.NewErrInvalidInput(result.Message)
		default:
			return nil, nil, types.NewErrClient(fmt.Errorf("unexpected result type: %T", result))
		}
	}

	return internal.PaginationExecutor(ctx, loadPageFn, roleAssignmentsEdgeFn)
}

// ListRoleAssignmentsOnDataSource returns a list of role assignments for a given role on a given data source
// The order of the list can be specified with WithRoleAssignmentListOrder.
// A filter can be specified with WithRoleAssignmentListFilter.
// A channel is returned that can be used to receive the list of types.RoleAssignment.
// To close the channel ensure to cancel the context.
func (c *RoleClient) ListRoleAssignmentsOnDataSource(ctx context.Context, dataSourceId string, ops ...func(*RoleAssignmentListOptions)) iter.Seq2[*types.RoleAssignment, error] { //nolint:dupl
	options := RoleAssignmentListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.RoleAssignmentConnectionEdgesRoleAssignmentEdge, error) {
		output, err := schema.ListRoleAssignmentsOnDataSource(ctx, c.client, dataSourceId, cursor, new(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch ds := output.DataSource.(type) {
		case *schema.ListRoleAssignmentsOnDataSourceDataSource:
			switch ra := (ds.RoleAssignments).(type) {
			case *schema.ListRoleAssignmentsOnDataSourceDataSourceRoleAssignmentsRoleAssignmentConnection:
				return &ra.PageInfo.PageInfo, ra.Edges, nil
			case *schema.ListRoleAssignmentsOnDataSourceDataSourceRoleAssignmentsNotFoundError:
				return nil, nil, types.NewErrNotFound(dataSourceId, ds.Typename, ra.Message)
			case *schema.ListRoleAssignmentsOnDataSourceDataSourceRoleAssignmentsPermissionDeniedError:
				return nil, nil, types.NewErrPermissionDenied("listRoleAssignmentsOnDataSource", ra.Message)
			default:
				return nil, nil, fmt.Errorf("unexpected type '%T'", ds)
			}
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

// ListRoleAssignmentsOnAccessControl returns a list of role assignments for a given role on an access control.
// The order of the list can be specified with WithRoleAssignmentListOrder.
// A filter can be specified with WithRoleAssignmentListFilter.
// A channel is returned that can be used to receive the list of types.RoleAssignment.
// To close the channel ensure to cancel the context.
func (c *RoleClient) ListRoleAssignmentsOnAccessControl(ctx context.Context, accessControlId string, ops ...func(*RoleAssignmentListOptions)) iter.Seq2[*types.RoleAssignment, error] { //nolint:dupl
	options := RoleAssignmentListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.RoleAssignmentConnectionEdgesRoleAssignmentEdge, error) {
		output, err := schema.ListRoleAssignmentsOnAccessControl(ctx, c.client, accessControlId, cursor, new(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch ap := output.AccessControl.(type) {
		case *schema.ListRoleAssignmentsOnAccessControlAccessControl:
			roleAssignments := (ap.RoleAssignments).(*schema.ListRoleAssignmentsOnAccessControlAccessControlRoleAssignmentsRoleAssignmentConnection)
			return &roleAssignments.PageInfo.PageInfo, roleAssignments.Edges, nil
		case *schema.ListRoleAssignmentsOnAccessControlAccessControlPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("listRoleAssignmentsOnAccessControl", ap.Message)
		case *schema.ListRoleAssignmentsOnAccessControlAccessControlNotFoundError:
			return nil, nil, types.NewErrNotFound(accessControlId, ap.Typename, ap.Message)
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
func (c *RoleClient) ListRoleAssignmentsOnUser(ctx context.Context, userId string, ops ...func(*RoleAssignmentListOptions)) iter.Seq2[*types.RoleAssignment, error] {
	options := RoleAssignmentListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.RoleAssignmentConnectionEdgesRoleAssignmentEdge, error) {
		output, err := schema.ListRoleAssignmentsOnUser(ctx, c.client, userId, cursor, new(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch r := output.User.(type) {
		case *schema.ListRoleAssignmentsOnUserUser:
			switch ra := r.RoleAssignments.(type) {
			case *schema.ListRoleAssignmentsOnUserUserRoleAssignmentsRoleAssignmentConnection:
				return &ra.PageInfo.PageInfo, ra.Edges, nil
			case *schema.ListRoleAssignmentsOnUserUserRoleAssignmentsPermissionDeniedError:
				return nil, nil, types.NewErrPermissionDenied("listRoleAssignmentsOnUser", ra.Message)
			case *schema.ListRoleAssignmentsOnUserUserRoleAssignmentsNotFoundError:
				return nil, nil, types.NewErrNotFound(userId, ra.Typename, ra.Message)
			default:
				return nil, nil, fmt.Errorf("unexpected type '%T'", ra)
			}

		case *schema.ListRoleAssignmentsOnUserUserPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("listRoleAssignmentsOnUser", r.Message)
		case *schema.ListRoleAssignmentsOnUserUserNotFoundError:
			return nil, nil, types.NewErrNotFound(userId, r.Typename, r.Message)
		case *schema.ListRoleAssignmentsOnUserUserInvalidInputError:
			return nil, nil, types.NewErrInvalidInput(r.Message)
		default:
			return nil, nil, fmt.Errorf("unexpected type '%T'", r)
		}
	}

	return internal.PaginationExecutor(ctx, loadPageFn, roleAssignmentsEdgeFn)
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

// UpdateRoleAssigneesOnAccessControl updates a role assignment between an access control and a set of users.
// Existing role assignments will be overwritten.
// accessControlId is the id of the access provider to assign the role to.
// roleId is the id of the role to assign.
// assignees is a list of user ids to assign the role to.
func (c *RoleClient) UpdateRoleAssigneesOnAccessControl(ctx context.Context, accessControlId string, roleId string, assignees ...string) (*types.Role, error) {
	output, err := schema.UpdateRoleAssigneesOnAccessControl(ctx, c.client, accessControlId, roleId, assignees)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := output.UpdateRoleAssigneesOnAccessControl.(type) {
	case *schema.UpdateRoleAssigneesOnAccessControlUpdateRoleAssigneesOnAccessControlRole:
		return &response.Role, nil
	case *schema.UpdateRoleAssigneesOnAccessControlUpdateRoleAssigneesOnAccessControlPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("updateRoleAssigneesOnAccessControl", response.Message)
	case *schema.UpdateRoleAssigneesOnAccessControlUpdateRoleAssigneesOnAccessControlNotFoundError:
		return nil, types.NewErrNotFound(accessControlId, response.Typename, response.Message)
	default:
		return nil, fmt.Errorf("unexpected type '%T'", response)
	}
}

func roleAssignmentsEdgeFn(edge *types.RoleAssignmentConnectionEdgesRoleAssignmentEdge) (*string, *schema.RoleAssignment, error) {
	cursor := edge.Cursor
	if edge.Node == nil {
		return cursor, nil, nil
	}
	return cursor, &edge.Node.RoleAssignment, nil
}
