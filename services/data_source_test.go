package services_test

import (
	"encoding/json"
	"os"

	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/services"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

func createDataSource(suite *suite.Suite, dataSourceClient *services.DataSourceClient) *schema.DataSource {
	ctx := suite.T().Context()

	dataSourceName := "Test Data Source for Access Control " + uuid.New().String()
	dataSourceDescription := "SDK Tests DataSource Description"
	dataSource, err := dataSourceClient.CreateDataSource(ctx, schema.DataSourceInput{
		Name:        &dataSourceName,
		Description: &dataSourceDescription,
	})

	suite.Require().NoError(err, "Failed to create data source")
	suite.Require().NotNil(dataSource, "Failed to create data source")

	metadataJson, err := os.ReadFile("testdata/test_data_source.json")
	suite.Require().NoError(err, "Failed to read data source metadata file")
	var metadata schema.DataSourceMetaDataInput

	err = json.Unmarshal(metadataJson, &metadata)
	suite.Require().NoError(err, "Failed to unmarshal data source metadata")

	dataSourceWithMetaData, err := dataSourceClient.SetDataSourceMetadata(ctx, dataSource.Id, metadata)
	suite.Require().NoError(err, "Failed to set data source metadata")
	suite.NotNil(dataSourceWithMetaData, "Data source with metadata is nil")

	return dataSourceWithMetaData
}