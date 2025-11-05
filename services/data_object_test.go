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
	"github.com/stretchr/testify/suite"
)

type DataObjectServiceTestSuite struct {
	suite.Suite

	sdkClient         *sdk.CollibraClient
	createdDataSource *schema.DataSource
	dataObjectClient  *services.DataObjectClient
}

func (suite *DataObjectServiceTestSuite) SetupSuite() {
	config := utils.GetEnvConfig(&suite.Suite)
	sdkClient := sdk.NewClient(
		config.User,
		config.Password,
		config.URL,
	)

	suite.Require().NotNil(sdkClient, "Failed to create SDK client")
	suite.sdkClient = sdkClient

	// create data source
	dataSourceClient := sdkClient.DataSource()
	suite.Require().NotNil(dataSourceClient, "Failed to create Data Source client")

	suite.createdDataSource = createDataSource(&suite.Suite, dataSourceClient)
	// import data objects
	importDataObjects(suite)

	suite.dataObjectClient = sdkClient.DataObject()
	suite.Require().NotNil(suite.dataObjectClient, "Failed to create Data Object client")
}

func importDataObjects(suite *DataObjectServiceTestSuite) {
	dataObjectsJson, err := os.ReadFile("testdata/test_data_objects.json")
	suite.Require().NoError(err, "Failed to read data objects file")
	var dataObjects []schema.DataObjectImport

	err = json.Unmarshal(dataObjectsJson, &dataObjects)
	suite.Require().NoError(err, "Failed to unmarshal data objects json")

	commands := make([]schema.ImportCommand, 0, len(dataObjects))
	for i := range dataObjects {
		commands = append(commands, schema.ImportCommand{
			UpsertDataObject: &dataObjects[i],
		})
	}
	// import using imported client
	submitObjects(&suite.Suite, suite.sdkClient.Job(), suite.sdkClient.Importer(), suite.createdDataSource.Id, "DS", "DataObjectImport", commands)
}

func (suite *DataObjectServiceTestSuite) TearDownSuite() {
	ctx := suite.T().Context()
	err := suite.sdkClient.DataSource().DeleteDataSource(ctx, suite.createdDataSource.Id)
	suite.NoError(err, "Failed to delete data source")
}

func TestDataObjectServiceTestSuite(t *testing.T) {
	suite.Run(t, new(DataObjectServiceTestSuite))
}

func (suite *DataObjectServiceTestSuite) TestA_ListDataObjects() {
	ctx := suite.T().Context()
	dataObjectClient := suite.dataObjectClient
	expectedFullNames := []string{"RAITO_DBT", "RAITO_DBT.DEFAULT", "RAITO_DBT.DEFAULT.CUSTOMER", "RAITO_DBT.DEFAULT.CUSTOMER.LASTNAME"}
	sortingOrder := schema.SortAsc

	response := dataObjectClient.ListDataObjects(ctx, services.WithDataObjectListFilter(&schema.DataObjectFilterInput{
		DataSources: []string{suite.createdDataSource.Id},
	}), services.WithDataObjectListOrder(schema.DataObjectOrderByInput{
		FullName: &sortingOrder,
	}))
	fullNamesList := make([]string, 0)
	for dataObject, err := range response {
		suite.Require().NoError(err, "Error while iterating data objects")
		suite.Require().NotNil(dataObject, "Data object should not be nil")
		fullNamesList = append(fullNamesList, dataObject.FullName)
	}

	suite.Equal(len(expectedFullNames), len(fullNamesList), "Number of data objects does not match expected")
	for _, expected := range expectedFullNames {
		suite.Contains(fullNamesList, expected, "Data object name %s not found", expected)
	}

	// Verify ascending order
	suite.True(sort.StringsAreSorted(fullNamesList), "Names should be sorted in ascending order")
}

func (suite *DataObjectServiceTestSuite) TestB_GetDataObjectByName_Than_ById_And_CompareDetails() {
	ctx := suite.T().Context()
	dataObjectName := "RAITO_DBT.DEFAULT"
	dataObjectClient := suite.dataObjectClient

	dataObject, err := dataObjectClient.GetDataObjectIdByName(ctx, dataObjectName, suite.createdDataSource.Id, services.WithDataObjectByExternalIdIncludeDataSource())
	suite.NoError(err, "Failed to get data object by name")
	suite.NotNil(dataObject, "Data object should not be nil")

	dataObjectDetails, err := dataObjectClient.GetDataObject(ctx, dataObject)
	suite.NoError(err, "Failed to get data object details")
	suite.NotNil(dataObjectDetails, "Data object details should not be nil")

	suite.Equal(dataObjectName, dataObjectDetails.FullName, "Data object name should match")
	suite.Equal(suite.createdDataSource.Id, dataObjectDetails.DataSource.Id, "Data source ID should match")
}
