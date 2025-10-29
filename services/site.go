package services

import (
	"context"
	"fmt"

	"github.com/Khan/genqlient/graphql"
	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/types"
)

type SiteService struct {
	client graphql.Client
}

func NewSiteService(client graphql.Client) *SiteService {
	return &SiteService{
		client: client,
	}
}

func (s *SiteService) UpdateSiteSettings(ctx context.Context, siteInfo types.EdgeSiteInfoInput) error {
	response, err := schema.UpdateSiteSettings(ctx, s.client, siteInfo)
	if err != nil {
		return types.NewErrClient(err)
	}

	switch v := response.UpdateEdgeSiteParameterDefinitions.(type) {
	case *schema.UpdateSiteSettingsUpdateEdgeSiteParameterDefinitionsEdgeSiteUpdateResponse:
		return nil
	case *schema.UpdateSiteSettingsUpdateEdgeSiteParameterDefinitionsPermissionDeniedError:
		return types.NewErrPermissionDenied("updateSiteSettings", v.Message)
	default:
		return fmt.Errorf("unexpected response type: %T", v)
	}
}

func (s *SiteService) NextSyncJobForSite(ctx context.Context, siteId string) (*types.SyncJob, error) {
	response, err := schema.NextSyncJobForSite(ctx, s.client, siteId)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch v := response.NextSyncJobForSite.(type) {
	case *schema.NextSyncJobForSiteNextSyncJobForSiteSyncJob:
		return &v.SyncJob, nil
	case *schema.NextSyncJobForSiteNextSyncJobForSitePermissionDeniedError:
		return nil, types.NewErrPermissionDenied("nextSyncJobForSite", v.Message)
	case *schema.NextSyncJobForSiteNextSyncJobForSiteInvalidInputError:
		return nil, types.NewErrInvalidInput(v.Message)
	case *schema.NextSyncJobForSiteNextSyncJobForSiteNotFoundError:
		return nil, types.NewErrNotFound(siteId, nil, v.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", v)
	}
}
