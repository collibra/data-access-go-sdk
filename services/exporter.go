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
	OnlyOutOfSync bool
}

func WithExportOutOfSyncOnly() func(options *ExportOptions) {
	return func(options *ExportOptions) {
		options.OnlyOutOfSync = true
	}
}

// StartExportFlow starts a new export flow.
func (c *ExporterClient) StartExportFlow(ctx context.Context, flowId uuid.UUID, ops ... func(*ExportOptions)) (*types.StartExportFlow, error) {
	options := ExportOptions{}
	for _, op := range ops {
		op(&options)
	}

	result, err := schema.TriggerExportFlow(ctx, c.client, flowId, types.ExportFlowOptions{
		OnlyOutOfSync: &options.OnlyOutOfSync,
	})
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
func (c *ExporterClient) FinishExportFlow(ctx context.Context, flowId uuid.UUID, startTime time.Time) (bool, error) {
	result, err := schema.FinalizeExportFlow(ctx, c.client, flowId, startTime)
	if err != nil {
		return false, types.NewErrClient(err)
	}

	return result.FinishExportFlow, nil
}

type FetchExportAccessControlsParams struct {
	StartSequenceId int
	LastSequenceId  *int
}

type FetchExportAccessControlsOption func(params *FetchExportAccessControlsParams)

func WithFetchExportAccessControlsLastSequenceId(lastSequenceId int) FetchExportAccessControlsOption {
	return func(params *FetchExportAccessControlsParams) {
		params.LastSequenceId = &lastSequenceId
	}
}

func WithFetchExportAccessControlsStartSequenceId(startSequenceId int) FetchExportAccessControlsOption {
	return func(params *FetchExportAccessControlsParams) {
		params.StartSequenceId = startSequenceId
	}
}

// FetchExportAccessControls streams the access controls exported in the given flow.
func (c *ExporterClient) FetchExportAccessControls(ctx context.Context, flowId uuid.UUID, ops ...FetchExportAccessControlsOption) iter.Seq2[types.ExportedItem, error] {
	return func(yield func(types.ExportedItem, error) bool) {
		options := FetchExportAccessControlsParams{}

		for _, op := range ops {
			op(&options)
		}

		after := options.StartSequenceId

		for {
			result, err := schema.FetchExportAccessControls(ctx, c.client, flowId, &after)
			if err != nil {
				yield(nil, types.NewErrClient(err))
				break
			}

			var controls *types.ExportAccessControls
			var fetchErr error

			switch response := result.FetchExportAccessControls.(type) {
			case *schema.FetchExportAccessControlsFetchExportAccessControls:
				controls = &response.ExportAccessControls
				after = controls.LastSequenceId
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

			for i := range controls.AccessControls {
				var toReturn types.ExportedItem

				switch ac := controls.AccessControls[i].(type) {
				case *types.ExportAccessControlsAccessControlsExportAccessControl:
					toReturn = &types.ExportedItemExportAccessControl{
						ExportAccessControl: ac.ExportAccessControl,
					}
				case *types.ExportAccessControlsAccessControlsExportColumnMask:
					toReturn = &types.ExportedItemExportColumnMask{
						ExportColumnMask: ac.ExportColumnMask,
					}
				default:
					yield(nil, fmt.Errorf("unknown exported access control type: %T", ac))

					return
				}

				if !yield(toReturn, nil) {
					return
				}
			}

			if (options.LastSequenceId != nil && *options.LastSequenceId == controls.LastSequenceId) || len(controls.AccessControls) == 0 {
				return
			}
		}
	}
}
