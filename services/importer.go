package services

import (
	"context"
	"fmt"

	"github.com/Khan/genqlient/graphql"
	"github.com/google/uuid"

	"github.com/collibra/access-governance-go-sdk/internal/schema"
	"github.com/collibra/access-governance-go-sdk/types"
)

type ImporterClient struct {
	client graphql.Client
}

func NewImporterClient(client graphql.Client) *ImporterClient {
	return &ImporterClient{
		client: client,
	}
}

// StartImportFlow starts a new import flow.
func (c *ImporterClient) StartImportFlow(ctx context.Context, input types.StartInputFlowInput) (*types.Subtask, error) {
	result, err := schema.StartImportFlow(ctx, c.client, input)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.StartImportFlow.(type) {
	case *schema.StartImportFlowStartImportFlowSubtask:
		return &response.Subtask, nil
	case *schema.StartImportFlowStartImportFlowPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("startImportFlow", response.Message)
	case *schema.StartImportFlowStartImportFlowNotFoundError:
		return nil, types.NewErrNotFound("", response.Typename, response.Message)
	case *schema.StartImportFlowStartImportFlowInvalidInputError:
		return nil, types.NewErrInvalidInput(response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", response)
	}
}

// FinishImportFlow finishes an import flow.
func (c *ImporterClient) FinishImportFlow(ctx context.Context, flowId uuid.UUID) error {
	_, err := schema.FinishImportFlow(ctx, c.client, flowId)
	if err != nil {
		return types.NewErrClient(err)
	}
	return nil
}

// SubmitImportObjects submits objects to an import flow.
func (c *ImporterClient) SubmitImportObjects(ctx context.Context, input types.ImportCommands) (*types.SubmittedCommands, error) {
	result, err := schema.SubmitImportObjects(ctx, c.client, input)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.SubmitImportObjects.(type) {
	case *schema.SubmitImportObjectsSubmitImportObjectsSubmittedCommands:
		return &response.SubmittedCommands, nil
	case *schema.SubmitImportObjectsSubmitImportObjectsPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("submitImportObjects", response.Message)
	case *schema.SubmitImportObjectsSubmitImportObjectsNotFoundError:
		return nil, types.NewErrNotFound("", response.Typename, response.Message)
	case *schema.SubmitImportObjectsSubmitImportObjectsInvalidInputError:
		return nil, types.NewErrInvalidInput(response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", response)
	}
}
