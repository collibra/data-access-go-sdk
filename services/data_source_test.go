package services_test

import (
	"encoding/json"
	"os"
	"sort"
	"testing"

	sdk "github.com/collibra/data-access-go-sdk"
	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/services"
	"github.com/collibra/data-access-go-sdk/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

func readDataSourceMetadata(suite *suite.Suite) schema.DataSourceMetaDataInput {
	metadataJson, err := os.ReadFile("testdata/test_data_source.json")
	suite.Require().NoError(err, "Failed to read data source metadata file")
	var metadata schema.DataSourceMetaDataInput

	err = json.Unmarshal(metadataJson, &metadata)
	suite.Require().NoError(err, "Failed to unmarshal data source metadata")

	return metadata
}

func createDataSource(suite *suite.Suite, dataSourceClient *services.DataSourceClient, input *schema.DataSourceInput) *schema.DataSource {
	ctx := suite.T().Context()

	if input == nil {
		dataSourceName := "Test Data Source " + uuid.New().String()
		dataSourceDescription := "SDK Tests DataSource Description"
		input = &schema.DataSourceInput{
			Name:        &dataSourceName,
			Description: &dataSourceDescription,
		}
	}

	dataSource, err := dataSourceClient.CreateDataSource(ctx, *input)

	suite.Require().NoError(err, "Failed to create data source")
	suite.Require().NotNil(dataSource, "Failed to create data source")

	return dataSource
}

func setDataSourceMetadata(suite *suite.Suite, dataSourceClient *services.DataSourceClient, dataSourceId string, metadata *schema.DataSourceMetaDataInput) *schema.DataSource {
	ctx := suite.T().Context()
	suite.Require().NotNil(dataSourceId, "dataSourceId is nil")

	if metadata == nil {
		metadataValue := readDataSourceMetadata(suite)
		metadata = &metadataValue
	}

	dataSourceWithMetaData, err := dataSourceClient.SetDataSourceMetadata(ctx, dataSourceId, *metadata)
	suite.Require().NoError(err, "Failed to set data source metadata")
	suite.NotNil(dataSourceWithMetaData, "Data source with metadata is nil")

	return dataSourceWithMetaData
}

type DataSourceServiceTestSuite struct {
	suite.Suite

	createdDataSource *schema.DataSource
	parentDataSource  *schema.DataSource
	dataSourceClient  *services.DataSourceClient
	metadata          *schema.DataSourceMetaDataInput
}

func TestDataSourceServiceTestSuite(t *testing.T) {
	suite.Run(t, new(DataSourceServiceTestSuite))
}

func (suite *DataSourceServiceTestSuite) SetupSuite() {
	config := utils.GetEnvConfig(&suite.Suite)
	sdkClient := sdk.NewClient(
		config.User,
		config.Password,
		config.URL,
	)

	suite.Require().NotNil(sdkClient, "Failed to create SDK client")

	dataSourceClient := sdkClient.DataSource()
	suite.Require().NotNil(dataSourceClient, "Failed to create Data Source client")
	suite.dataSourceClient = dataSourceClient
	metadata := readDataSourceMetadata(&suite.Suite)
	suite.metadata = &metadata
}

func (suite *DataSourceServiceTestSuite) TestA_CreateDataSource_WithParent() {
	dataSourceClient := suite.dataSourceClient
	parentDataSource := createDataSource(&suite.Suite, dataSourceClient, nil)
	suite.parentDataSource = parentDataSource

	dataSourceName := "Test Data Source " + uuid.New().String()
	dataSourceDescription := "SDK Tests DataSource Description"
	input := &schema.DataSourceInput{
		Name:        &dataSourceName,
		Description: &dataSourceDescription,
		Parent:      &parentDataSource.Id,
	}

	createdDataSource := createDataSource(&suite.Suite, dataSourceClient, input)
	suite.Equal(dataSourceName, createdDataSource.Name)
	suite.Equal(dataSourceDescription, createdDataSource.Description)
	suite.createdDataSource = createdDataSource
}

func (suite *DataSourceServiceTestSuite) TestB_ListDataSources_WithDataSourceListFilter() {
	ctx := suite.T().Context()
	if suite.createdDataSource == nil || suite.parentDataSource == nil {
		suite.T().Log("Skipping TestB_ListDataSources as no data sources were created")
		suite.T().SkipNow()
	}

	response := suite.dataSourceClient.ListDataSources(ctx, services.WithDataSourceListFilter(&schema.DataSourceFilterInput{
		Parent: &suite.parentDataSource.Id,
	}))
	for ds, err := range response {
		suite.Require().NoError(err, "Error while listing data sources")
		suite.NotNil(ds, "Data source is nil")
	}
}

func (suite *DataSourceServiceTestSuite) TestC_ListDataSources_WithDataSourceListSearch_And_WithDataSourceListOrder() {
	ctx := suite.T().Context()
	if suite.createdDataSource == nil || suite.parentDataSource == nil {
		suite.T().Log("Skipping TestC_ListDataSources_WithDataSourceListSearch_And_WithDataSourceListOrder as no data sources were created")
		suite.T().SkipNow()
	}

	searchString := "Test"
	listOrder := schema.SortDesc
	listOrderInput := schema.DataSourceOrderByInput{
		Name: &listOrder,
	}

	response := suite.dataSourceClient.ListDataSources(ctx,
		services.WithDataSourceListSearch(&searchString),
		services.WithDataSourceListOrder(listOrderInput),
	)
	availableNames := make([]string, 0)

	for ds, err := range response {
		suite.Require().NoError(err, "Error while listing data sources with search and order")
		suite.NotNil(ds, "Data source is nil")
		availableNames = append(availableNames, ds.Name)
	}

	suite.Contains(availableNames, suite.createdDataSource.Name, "Created data source name not found in the list")
	suite.Contains(availableNames, suite.parentDataSource.Name, "Parent data source name not found in the list")
	suite.True(sort.IsSorted(sort.Reverse(sort.StringSlice(availableNames))), "Data source names are not sorted in descending order")
}

func (suite *DataSourceServiceTestSuite) TestD_UpdateDataSource() {
	ctx := suite.T().Context()

	if suite.createdDataSource == nil {
		suite.T().Log("Skipping TestD_UpdateDataSource as no data sources were created")
		suite.T().SkipNow()
	}

	createDataSource := suite.createdDataSource

	newName := "Updated" + createDataSource.Name

	updatedDataSource, err := suite.dataSourceClient.UpdateDataSource(ctx, createDataSource.Id, schema.DataSourceInput{
		Name: &newName,
	})
	suite.Require().NoError(err, "Failed to update data source")
	suite.Require().NotNil(updatedDataSource, "Updated data source is nil")
	suite.Equal(newName, updatedDataSource.Name, "Data source name was not updated")
	suite.createdDataSource = updatedDataSource
}

func (suite *DataSourceServiceTestSuite) TestE_GetDataSource() {
	ctx := suite.T().Context()

	if suite.createdDataSource == nil {
		suite.T().Log("Skipping TestE_GetDataSource as no data sources were created")
		suite.T().SkipNow()
	}

	createDataSource := suite.createdDataSource

	retrievedDataSource, err := suite.dataSourceClient.GetDataSource(ctx, createDataSource.Id)
	suite.Require().NoError(err, "Failed to get data source")
	suite.Require().NotNil(retrievedDataSource, "Retrieved data source is nil")
	suite.Require().Equal(createDataSource, retrievedDataSource)
}

func (suite *DataSourceServiceTestSuite) TestF_SetMaskingMetadata() {
	if suite.createdDataSource == nil || suite.metadata == nil {
		suite.T().Log("Skipping TestF_SetMaskingMetadata as no data sources were created or metadata is nil")
		suite.T().SkipNow()
	}

	metadata := suite.metadata
	dataSourceWithMetadata := setDataSourceMetadata(&suite.Suite, suite.dataSourceClient, suite.createdDataSource.Id, metadata)
	suite.Equal(suite.createdDataSource.Id, dataSourceWithMetadata.Id, "Data source ID should match")
}

func (suite *DataSourceServiceTestSuite) TestG_GetMaskingMetadata() {
	if suite.createdDataSource == nil || suite.metadata == nil {
		suite.T().Log("Skipping TestG_GetMaskingMetadata as no data sources were created or metadata is nil")
		suite.T().SkipNow()
	}

	retrievedMaskingMetaData, err := suite.dataSourceClient.GetMaskingMetadata(suite.T().Context(), suite.createdDataSource.Id)
	suite.Require().NoError(err, "Failed to get masking metadata")
	suite.Require().NotNil(retrievedMaskingMetaData, "Retrieved masking metadata is nil")
	suite.NotEmpty(retrievedMaskingMetaData.MaskTypes, "Mask types should not be empty")
	suite.Equal(*suite.metadata.MaskingMetadata.DefaultMaskExternalName, *retrievedMaskingMetaData.DefaultMaskExternalName, "Default mask external name should match")
}

func (suite *DataSourceServiceTestSuite) TestH_DeleteDataSource() {
	ctx := suite.T().Context()

	if suite.createdDataSource == nil {
		suite.T().Log("Skipping TestH_DeleteDataSource as no data sources were created")
		suite.T().SkipNow()
	}

	err := suite.dataSourceClient.DeleteDataSource(ctx, suite.createdDataSource.Id)
	suite.Require().NoError(err, "Failed to delete data source")

	// Verify deletion by attempting to get the data source
	_, err = suite.dataSourceClient.GetDataSource(ctx, suite.createdDataSource.Id)
	suite.Require().Error(err, "Expected error when getting deleted data source")
}

func (suite *DataSourceServiceTestSuite) TestI_DeleteParentDataSource() {
	ctx := suite.T().Context()

	if suite.parentDataSource == nil {
		suite.T().Log("Skipping TestI_DeleteParentDataSource as no parent data source was created")
		suite.T().SkipNow()
	}

	err := suite.dataSourceClient.DeleteDataSource(ctx, suite.parentDataSource.Id)
	suite.Require().NoError(err, "Failed to delete parent data source")
}

func (suite *DataSourceServiceTestSuite) TestJ_GetDataSource_NotFound() {
	ctx := suite.T().Context()
	nonExistentId := "non-existent-id"

	_, err := suite.dataSourceClient.GetDataSource(ctx, nonExistentId)
	suite.Require().Error(err, "Expected error when getting non-existent data source")
}
