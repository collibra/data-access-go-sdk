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

func (s *SiteService) NextSyncJobForEdgeDataSource(ctx context.Context, syncInput types.SyncJobForEdgeDataSourceInput) (*types.SyncJob, error) {
	response, err := schema.NextSyncJobForEdgeDataSource(ctx, s.client, syncInput)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch v := response.NextSyncJobForEdgeDataSource.(type) {
	case *schema.NextSyncJobForEdgeDataSourceNextSyncJobForEdgeDataSourceSyncJob:
		return &v.SyncJob, nil
	case *schema.NextSyncJobForEdgeDataSourceNextSyncJobForEdgeDataSourcePermissionDeniedError:
		return nil, types.NewErrPermissionDenied("nextSyncJobForSite", v.Message)
	case *schema.NextSyncJobForEdgeDataSourceNextSyncJobForEdgeDataSourceInvalidInputError:
		return nil, types.NewErrInvalidInput(v.Message)
	case *schema.NextSyncJobForEdgeDataSourceNextSyncJobForEdgeDataSourceNotFoundError:
		return nil, types.NewErrNotFound(syncInput.DataSourceId, nil, v.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", v)
	}
}

func (s *SiteService) SiteInfo(ctx context.Context, siteId string) (*types.EdgeSiteInfoResult, error) {
	response, err := schema.EdgeSiteInfo(ctx, s.client, siteId)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch v := response.EdgeSiteInfo.(type) {
	case *schema.EdgeSiteInfoEdgeSiteInfoEdgeSiteInfoResponse:
		return &v.EdgeSiteInfoResult, nil
	case *schema.EdgeSiteInfoEdgeSiteInfoPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("edgeSiteInfo", v.Message)
	case *schema.EdgeSiteInfoEdgeSiteInfoNotFoundError:
		return nil, types.NewErrNotFound(siteId, nil, v.Message)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", v)
	}
}
