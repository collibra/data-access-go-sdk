package services

import (
	"context"

	"github.com/Khan/genqlient/graphql"
	"github.com/aws/smithy-go/ptr"

	"github.com/raito-io/sdk-go/internal"
	"github.com/raito-io/sdk-go/internal/schema"
	"github.com/raito-io/sdk-go/types"
)

type GroupClient struct {
	client graphql.Client
}

func NewGroupClient(client graphql.Client) GroupClient {
	return GroupClient{
		client: client,
	}
}

// GetGroup returns the group with the given ID.
func (g GroupClient) GetGroup(ctx context.Context, id string) (*types.Group, error) {
	result, err := schema.GetGroup(ctx, g.client, id)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	return &result.Group.Group, nil
}

type GroupListOptions struct {
	order  []types.GroupOrderByInput
	filter *types.GroupFilterInput
}

// WithGroupListOrder sets the order of the returned Groups in the ListGroups call
func WithGroupListOrder(input ...types.GroupOrderByInput) func(options *GroupListOptions) {
	return func(options *GroupListOptions) {
		options.order = append(options.order, input...)
	}
}

// WithGroupListFilter sets the filter of the returned Groups in the ListGroups call
func WithGroupListFilter(input *types.GroupFilterInput) func(options *GroupListOptions) {
	return func(options *GroupListOptions) {
		options.filter = input
	}
}

// ListGroups returns a list of Groups
// The order of the list can be specified with WithGroupListOrder
// A filter can be specified with WithGroupListFilter
// A channel is returned that can be used to receive the list of GroupListItem
// To close the channel ensure to cancel the context
func (g GroupClient) ListGroups(ctx context.Context, ops ...func(options *GroupListOptions)) <-chan types.ListItem[types.Group] { //nolint:dupl
	options := GroupListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.GroupPageEdgesEdge, error) {
		output, err := schema.ListGroups(ctx, g.client, cursor, ptr.Int(internal.MaxPageSize), options.filter, options.order)
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		return &output.Groups.PageInfo.PageInfo, output.Groups.Edges, nil
	}

	edgeFn := func(edge *types.GroupPageEdgesEdge) (*string, *schema.Group, error) {
		cursor := edge.Cursor

		if edge.Node == nil {
			return cursor, nil, nil
		}

		listItem := (*edge.Node).(*types.GroupPageEdgesEdgeNodeGroup)

		return cursor, &listItem.Group, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, edgeFn)
}
