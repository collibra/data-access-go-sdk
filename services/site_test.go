package services_test

import (
	"testing"

	sdk "github.com/collibra/data-access-go-sdk"
	"github.com/collibra/data-access-go-sdk/services"
	"github.com/collibra/data-access-go-sdk/types"
	"github.com/collibra/data-access-go-sdk/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

type SiteServiceTestSuite struct {
	suite.Suite

	SiteClient *services.SiteService

	siteId       uuid.UUID
	connectionId uuid.UUID
	dataSourceId string
}

func TestSiteServiceTestSuite(t *testing.T) {
	suite.Run(t, new(SiteServiceTestSuite))
}

func (suite *SiteServiceTestSuite) SetupSuite() {
	url, clientOptions := utils.GetEnvConfig(&suite.Suite)
	sdkClient := sdk.NewClient(url, clientOptions...)

	suite.Require().NotNil(sdkClient, "Failed to create SDK client")
	suite.SiteClient = sdkClient.Site()

	suite.siteId, _ = uuid.NewRandom()
	suite.connectionId, _ = uuid.NewRandom()
	suite.dataSourceId = "datasource-does-not-exist"
}

func (suite *SiteServiceTestSuite) Test_NextSyncJobForEdgeDataSource() {
	ctx := suite.T().Context()
	syncInput := types.SyncJobForEdgeDataSourceInput{
		EdgeSiteId:       suite.siteId.String(),
		EdgeConnectionId: suite.connectionId.String(),
		DataSourceId:     suite.dataSourceId,
	}
	_, err := suite.SiteClient.NextSyncJobForEdgeDataSource(ctx, syncInput)
	suite.Require().Error(err)
}
