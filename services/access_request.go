package services

import (
	"context"
	"fmt"

	"github.com/Khan/genqlient/graphql"

	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/types"
)

type AccessRequestClient struct {
	client graphql.Client
}

func NewAccessRequestClient(client graphql.Client) *AccessRequestClient {
	return &AccessRequestClient{
		client: client,
	}
}

// CreateAccessRequest creates a new AccessRequest.
// The newly created AccessRequest is returned if successful.
// Otherwise, an error is returned.
func (a *AccessRequestClient) CreateAccessRequest(ctx context.Context, input types.AccessRequestInput) (*types.AccessRequest, error) {
	result, err := schema.CreateAccessRequest(ctx, a.client, input)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.CreateAccessRequest.(type) {
	case *schema.CreateAccessRequestCreateAccessRequest:
		return &response.AccessRequest, nil
	case *schema.CreateAccessRequestCreateAccessRequestPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("createAccessRequest", response.Message)
	case *schema.CreateAccessRequestCreateAccessRequestNotFoundError:
		return nil, types.NewErrNotFound("", response.Typename, response.Message)
	case *schema.CreateAccessRequestCreateAccessRequestInvalidInputError:
		return nil, types.NewErrInvalidInput(response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", result.CreateAccessRequest)
	}
}
