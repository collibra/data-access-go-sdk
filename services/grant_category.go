package services

import (
	"context"
	"fmt"

	"github.com/Khan/genqlient/graphql"

	"github.com/raito-io/sdk-go/internal/schema"
	"github.com/raito-io/sdk-go/types"
)

type GrantCategoryClient struct {
	client graphql.Client
}

func NewGrantCategoryClient(client graphql.Client) GrantCategoryClient {
	return GrantCategoryClient{
		client: client,
	}
}

// CreateGrantCategory creates a new GrantCategory.
// The newly created GrantCategory is returned if successful.
// Otherwise, an error is returned.
func (a *GrantCategoryClient) CreateGrantCategory(ctx context.Context, category types.GrantCategoryInput) (*types.GrantCategoryDetails, error) {
	result, err := schema.CreateGrantCategory(ctx, a.client, category)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.CreateGrantCategory.(type) {
	case *schema.CreateGrantCategoryCreateGrantCategory:
		return &response.GrantCategoryDetails, nil
	case *schema.CreateGrantCategoryCreateGrantCategoryPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("createGrantCategory", response.Message)
	case *schema.CreateGrantCategoryCreateGrantCategoryNotFoundError:
		return nil, types.NewErrNotFound("", response.Typename, response.Message)
	case *schema.CreateGrantCategoryCreateGrantCategoryInvalidInputError:
		return nil, types.NewErrInvalidInput(response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", result.CreateGrantCategory)
	}
}

// UpdateGrantCategory updates an existing GrantCategory.
// The updated GrantCategory is returned if successful.
// Otherwise, an error is returned.
func (a *GrantCategoryClient) UpdateGrantCategory(ctx context.Context, id string, category types.GrantCategoryInput) (*types.GrantCategoryDetails, error) {
	result, err := schema.UpdateGrantCategory(ctx, a.client, id, category)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.UpdateGrantCategory.(type) {
	case *schema.UpdateGrantCategoryUpdateGrantCategory:
		return &response.GrantCategoryDetails, nil
	case *schema.UpdateGrantCategoryUpdateGrantCategoryPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("updateGrantCategory", response.Message)
	case *schema.UpdateGrantCategoryUpdateGrantCategoryNotFoundError:
		return nil, types.NewErrNotFound(id, response.Typename, response.Message)
	case *schema.UpdateGrantCategoryUpdateGrantCategoryInvalidInputError:
		return nil, types.NewErrInvalidInput(response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", result.UpdateGrantCategory)
	}
}

// DeleteGrantCategory deletes an existing GrantCategory.
// Returns nil if successful.
// Otherwise, returns an error.
func (a *GrantCategoryClient) DeleteGrantCategory(ctx context.Context, id string) error {
	result, err := schema.DeleteGrantCategory(ctx, a.client, id)
	if err != nil {
		return types.NewErrClient(err)
	}

	switch response := result.DeleteGrantCategory.(type) {
	case *schema.DeleteGrantCategoryDeleteGrantCategory:
		if response.GetSuccess() {
			return nil
		} else {
			return types.NewErrClient(fmt.Errorf("deleteGrantCategory failed"))
		}
	case *schema.DeleteGrantCategoryDeleteGrantCategoryPermissionDeniedError:
		return types.NewErrPermissionDenied("deleteGrantCategory", response.Message)
	case *schema.DeleteGrantCategoryDeleteGrantCategoryNotFoundError:
		return types.NewErrNotFound(id, response.Typename, response.Message)
	case *schema.DeleteGrantCategoryDeleteGrantCategoryInvalidInputError:
		return types.NewErrInvalidInput(response.Message)
	default:
		return fmt.Errorf("unexpected response type: %T", result.DeleteGrantCategory)
	}
}

// GetGrantCategory retrieves an existing GrantCategory.
// Returns the GrantCategory if successful.
// Otherwise, returns an error.
func (a *GrantCategoryClient) GetGrantCategory(ctx context.Context, id string) (*types.GrantCategoryDetails, error) {
	result, err := schema.GetGrantCategory(ctx, a.client, id)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.GrantCategory.(type) {
	case *schema.GetGrantCategoryGrantCategory:
		return &response.GrantCategoryDetails, nil
	case *schema.GetGrantCategoryGrantCategoryPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("getGrantCategory", response.Message)
	case *schema.GetGrantCategoryGrantCategoryNotFoundError:
		return nil, types.NewErrNotFound(id, response.Typename, response.Message)
	case *schema.GetGrantCategoryGrantCategoryInvalidInputError:
		return nil, types.NewErrInvalidInput(response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", result.GetGrantCategory)
	}
}

// ListGrantCategories retrieves all GrantCategories
// Returns a list of GrantCategories if successful.
// Otherwise, returns an error.
func (a *GrantCategoryClient) ListGrantCategories(ctx context.Context) ([]types.GrantCategoryDetails, error) {
	result, err := schema.ListGrantCategories(ctx, a.client)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	grantCategories := make([]types.GrantCategoryDetails, 0, len(result.GrantCategories))

	for i := range result.GrantCategories {
		grantCategories = append(grantCategories, result.GrantCategories[i].GrantCategoryDetails)
	}

	return grantCategories, nil
}
