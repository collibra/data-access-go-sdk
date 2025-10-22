package services

import (
	"context"
	"fmt"
	"time"

	"github.com/Khan/genqlient/graphql"
	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/google/uuid"

	"github.com/collibra/data-access-go-sdk/types"
)

type ExporterClient struct {
	client graphql.Client
}

func NewExporterClient(client graphql.Client) *ExporterClient {
	return &ExporterClient{
		client: client,
	}
}

type ExportOptions struct {
	OutOfSyncOnly bool
}

func WithExportOutOfSyncOnly() func(options *ExportOptions) {
	return func(options *ExportOptions) {
		options.OutOfSyncOnly = true
	}
}

// StartExportFlow starts a new export flow.
func (c *ExporterClient) StartExportFlow(ctx context.Context, input types.StartExportFlowInput) (*types.StartExportFlow, error) {
	result, err := schema.TriggerExportFlow(ctx, c.client, input)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.StartExportFlow.(type) {
	case *schema.TriggerExportFlowStartExportFlow:
		return &response.StartExportFlow, nil
	case *schema.TriggerExportFlowStartExportFlowPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("startExportFlow", response.Message)
	case *schema.TriggerExportFlowStartExportFlowNotFoundError:
		return nil, types.NewErrNotFound("", response.Typename, response.Message)
	case *schema.TriggerExportFlowStartExportFlowInvalidInputError:
		return nil, types.NewErrInvalidInput(response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", response)
	}
}

// FinishExportFlow finishes an export flow.
func (c *ExporterClient) FinishExportFlow(ctx context.Context, startTime time.Time, flowId uuid.UUID) (bool, error) {
	result, err := schema.FinalizeExportFlow(ctx, c.client, startTime, flowId)
	if err != nil {
		return false, types.NewErrClient(err)
	}

	return result.FinishExportFlow, nil
}

// FinishExportFlow finishes an export flow.
func (c *ExporterClient) FetchExportAccessControls(ctx context.Context, flowId uuid.UUID, after *int) (*types.ExportAccessControls, error) {
	result, err := schema.FetchExportAccessControls(ctx, c.client, flowId, after)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch response := result.FetchExportAccessControls.(type) {
	case *schema.FetchExportAccessControlsFetchExportAccessControls:
		return &response.ExportAccessControls, nil
	case *schema.FetchExportAccessControlsFetchExportAccessControlsPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("fetchExportAccessControls", response.Message)
	case *schema.FetchExportAccessControlsFetchExportAccessControlsNotFoundError:
		return nil, types.NewErrNotFound("", response.Typename, response.Message)
	case *schema.FetchExportAccessControlsFetchExportAccessControlsInvalidInputError:
		return nil, types.NewErrInvalidInput(response.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", response)
	}
}
