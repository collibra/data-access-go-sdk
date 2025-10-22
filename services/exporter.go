package services

import (
	"context"
	"fmt"
	"iter"
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

// FetchExportAccessControls streams the access controls exported in the given flow.
func (c *ExporterClient) FetchExportAccessControls(ctx context.Context, flowId uuid.UUID, lastSequenceId int) iter.Seq2[*types.ExportAccessControl, error] {
	return func(yield func(*types.ExportAccessControl, error) bool) {
		var after *int

		for {
			result, err := schema.FetchExportAccessControls(ctx, c.client, flowId, after)
			if err != nil {
				yield(nil, types.NewErrClient(err))
				break
			}

			var controls *types.ExportAccessControls
			var fetchErr error

			switch response := result.FetchExportAccessControls.(type) {
			case *schema.FetchExportAccessControlsFetchExportAccessControls:
				controls = &response.ExportAccessControls
			case *schema.FetchExportAccessControlsFetchExportAccessControlsPermissionDeniedError:
				fetchErr = types.NewErrPermissionDenied("fetchExportAccessControls", response.Message)
			case *schema.FetchExportAccessControlsFetchExportAccessControlsNotFoundError:
				fetchErr = types.NewErrNotFound("", response.Typename, response.Message)
			case *schema.FetchExportAccessControlsFetchExportAccessControlsInvalidInputError:
				fetchErr = types.NewErrInvalidInput(response.Message)
			default:
				fetchErr = fmt.Errorf("unexpected response type: %T", response)
			}

			if fetchErr != nil {
				yield(nil, fetchErr)
				return
			}

			for _, control := range controls.AccessControls { //nolint:gocritic
				if !yield(&control.ExportAccessControl, nil) {
					return
				}
			}

			if controls.LastSequenceId == lastSequenceId {
				return
			}
		}
	}
}
