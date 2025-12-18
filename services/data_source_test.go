package services_test

import (
	"encoding/json"
	"os"
	"sort"
	"testing"

	sdk "github.com/collibra/data-access-go-sdk"
	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/services"
	"github.com/collibra/data-access-go-sdk/types"
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

	dataSourceClient *services.DataSourceClient
	metadata         *schema.DataSourceMetaDataInput
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

func (suite *DataSourceServiceTestSuite) TestDataSources() {
	ctx := suite.T().Context()
	dataSourceClient := suite.dataSourceClient
	var createdDataSource *schema.DataSource
	var parentDataSource *schema.DataSource

	suite.Run("Create a Data Source With Parent", func() {
		parentDataSource = createDataSource(&suite.Suite, dataSourceClient, nil)

		dataSourceName := "Test Data Source " + uuid.New().String()
		dataSourceDescription := "SDK Tests DataSource Description"
		canRequestAccess := true
		canRequestAccessToTypes := []string{"table", "schema"}
		sampleCronExpression := "0 0 * * *"
		catalogSystemId := uuid.New()
		input := &schema.DataSourceInput{
			Name:        &dataSourceName,
			Description: &dataSourceDescription,
			Parent:      &parentDataSource.Id,
			CanRequestAccess: &canRequestAccess,
			CanRequestAccessToTypes: canRequestAccessToTypes,
			SyncSchedule: &schema.DataSourceSyncScheduleInput{
				Global: &sampleCronExpression,
				DataObjectSync: &sampleCronExpression,
				IdentitySync: &sampleCronExpression,
				AccessToTargetSync: &sampleCronExpression,
				AccessFromTargetSync: &sampleCronExpression,
				UsageSync: &sampleCronExpression,
			},
			CatalogSystemId: &catalogSystemId,
		}

		createdDataSource = createDataSource(&suite.Suite, dataSourceClient, input)
		suite.Equal(dataSourceName, createdDataSource.Name)
		suite.Equal(dataSourceDescription, createdDataSource.Description)
		suite.Require().NotNil(createdDataSource.Parent.Id)
		suite.Equal(parentDataSource.Id, createdDataSource.Parent.Id, "Parent for created Data Source does not match the original input")
	})

	suite.Run("List Data Sources With Data Source List Filter", func() {
		if createdDataSource == nil || parentDataSource == nil {
			suite.T().Log("Skipping List Data Sources With Data Source List Filter as no data sources were created")
			suite.T().SkipNow()
		}

		response := suite.dataSourceClient.ListDataSources(ctx, services.WithDataSourceListFilter(&schema.DataSourceFilterInput{
			Parent: &parentDataSource.Id,
		}))
		found := false
		for ds, err := range response {
			suite.Require().NoError(err, "Error while listing data sources")
			suite.NotNil(ds, "Data source is nil")

			if ds.Id == createdDataSource.Id {
				found = true
				break
			}
		}

		suite.Require().True(found, "Created data source not found in the list")
	})

	suite.Run("List Data Sources", func() {
		if createdDataSource == nil || parentDataSource == nil {
			suite.T().Log("Skipping List Data Sources With Data Source List Search And With Data Source List Order as no data sources were created")
			suite.T().SkipNow()
		}

		response := suite.dataSourceClient.ListDataSources(ctx)
		found := false

		for ds, err := range response {
			suite.Require().NoError(err, "Error while listing data sources with search and order")
			suite.NotNil(ds, "Data source is nil")

			if ds.Id == createdDataSource.Id {
				found = true
				break
			}
		}

		suite.Require().True(found, "Created data source not found in the list")
	})

	suite.Run("List Data Sources With Data Source List Search And With Data Source List Order", func() {
		if createdDataSource == nil || parentDataSource == nil {
			suite.T().Log("Skipping List Data Sources With Data Source List Search And With Data Source List Order as no data sources were created")
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

		suite.Contains(availableNames, createdDataSource.Name, "Created data source name not found in the list")
		suite.Contains(availableNames, parentDataSource.Name, "Parent data source name not found in the list")
		suite.True(sort.IsSorted(sort.Reverse(sort.StringSlice(availableNames))), "Data source names are not sorted in descending order")
	})

	suite.Run("Update Data Source", func() {
		if createdDataSource == nil {
			suite.T().Log("Skipping TestD_UpdateDataSource as no data sources were created")
			suite.T().SkipNow()
		}
		// newParent := createDataSource(&suite.Suite, dataSourceClient, nil) // TODO: Uncomment once https://engineering-collibra.atlassian.net/browse/DEV-154727 is fixed

		dataSourceName := "Test Data Source " + uuid.New().String()
		dataSourceDescription := "SDK Tests DataSource Description"
		canRequestAccess := false
		canRequestAccessToTypes := []string{"schema"}
		sampleCronExpression := "0 0 * * *"
		catalogSystemId := uuid.New()
		input := &schema.DataSourceInput{
			Name:        &dataSourceName,
			Description: &dataSourceDescription,
			// Parent: &newParent.Id, // TODO: Uncomment once https://engineering-collibra.atlassian.net/browse/DEV-154727 is fixed
			CanRequestAccess: &canRequestAccess,
			CanRequestAccessToTypes: canRequestAccessToTypes,
			SyncSchedule: &schema.DataSourceSyncScheduleInput{
				Global: &sampleCronExpression,
				DataObjectSync: &sampleCronExpression,
				IdentitySync: &sampleCronExpression,
				AccessToTargetSync: &sampleCronExpression,
				AccessFromTargetSync: &sampleCronExpression,
				UsageSync: &sampleCronExpression,
			},
			CatalogSystemId: &catalogSystemId,
		}

		updatedDataSource, err := suite.dataSourceClient.UpdateDataSource(ctx, createdDataSource.Id, *input)
		suite.Require().NoError(err, "Failed to update data source")
		suite.Require().NotNil(updatedDataSource, "Updated data source is nil")
		suite.Equal(*input.Name, updatedDataSource.Name, "Data source name was not updated")
		suite.Equal(dataSourceDescription, updatedDataSource.Description, "Description was not updated")
		// suite.Equal(newParent.Id, updatedDataSource.Parent.Id, "Parent was not updated") // TODO: Uncomment once https://engineering-collibra.atlassian.net/browse/DEV-154727 is fixed
		createdDataSource = updatedDataSource
	})

	suite.Run("Get Data Source", func() {
		if createdDataSource == nil {
			suite.T().Log("Skipping GetDataSource as no data sources were created")
			suite.T().SkipNow()
		}

		retrievedDataSource, err := suite.dataSourceClient.GetDataSource(ctx, createdDataSource.Id)
		suite.Require().NoError(err, "Failed to get data source")
		suite.Require().NotNil(retrievedDataSource, "Retrieved data source is nil")
		suite.Require().Equal(createdDataSource, retrievedDataSource)
	})

	suite.Run("Get Masking Metadata", func() {
		if createdDataSource == nil || suite.metadata == nil {
			suite.T().Log("Skipping TestF_GetMaskingMetadata as no data sources were created or metadata is nil")
			suite.T().SkipNow()
		}

		metadata := suite.metadata
		dataSourceWithMetadata := setDataSourceMetadata(&suite.Suite, dataSourceClient, createdDataSource.Id, metadata)
		suite.Equal(createdDataSource.Id, dataSourceWithMetadata.Id, "Data source ID should match")
	})

	suite.Run("Get Masking Metadata", func() {
		if createdDataSource == nil || suite.metadata == nil {
			suite.T().Log("Skipping TestF_GetMaskingMetadata as no data sources were created or metadata is nil")
			suite.T().SkipNow()
		}

		retrievedMaskingMetaData, err := suite.dataSourceClient.GetMaskingMetadata(suite.T().Context(), createdDataSource.Id)
		suite.Require().NoError(err, "Failed to get masking metadata")
		suite.Require().NotNil(retrievedMaskingMetaData, "Retrieved masking metadata is nil")
		suite.NotEmpty(retrievedMaskingMetaData.MaskTypes, "Mask types should not be empty")
		suite.Equal(*suite.metadata.MaskingMetadata.DefaultMaskExternalName, *retrievedMaskingMetaData.DefaultMaskExternalName, "Default mask external name should match")
	})

	suite.Run("Delete Data Source", func() {
		if createdDataSource == nil {
			suite.T().Log("Skipping TestH_DeleteDataSource as no data sources were created")
			suite.T().SkipNow()
		}

		err := dataSourceClient.DeleteDataSource(ctx, createdDataSource.Id)
		suite.Require().NoError(err, "Failed to delete data source")

		// Verify deletion by attempting to get the data source
		_, err = dataSourceClient.GetDataSource(ctx, createdDataSource.Id)
		suite.Require().Error(err, "Expected error when getting deleted data source")
	})

	suite.Run("Delete Parent Data Source", func() {
		if parentDataSource == nil {
			suite.T().Log("Skipping TestI_DeleteParentDataSource as no parent data source was created")
			suite.T().SkipNow()
		}

		err := dataSourceClient.DeleteDataSource(ctx, parentDataSource.Id)
		suite.Require().NoError(err, "Failed to delete parent data source")
	})
}

func (suite *DataSourceServiceTestSuite) TestGetUnexistingDataSource() {
	ctx := suite.T().Context()
	nonExistentId := "non-existent-id"

	_, err := suite.dataSourceClient.GetDataSource(ctx, nonExistentId)
	suite.Require().Error(err, "Expected error when getting non-existent data source")

	var notFoundErr *types.ErrNotFound
	suite.Require().ErrorAs(err, &notFoundErr)
	suite.Require().ErrorContains(notFoundErr, "Data source not found")
}

func (suite *DataSourceServiceTestSuite) TestDeleteNonExistentDataSource() {
	ctx := suite.T().Context()
	nonExistentId := "non-existent-id"

	err := suite.dataSourceClient.DeleteDataSource(ctx, nonExistentId)
	suite.Require().Error(err, "Expected error when deleting non-existent data source")

	var permErr *types.ErrPermissionDenied
	suite.Require().ErrorAs(err, &permErr)
	suite.Require().ErrorContains(permErr, "permission denied (data_source, delete) (non-existent-id)")
}
