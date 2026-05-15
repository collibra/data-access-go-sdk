package services_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	sdk "github.com/collibra/data-access-go-sdk"
	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/services"
	"github.com/collibra/data-access-go-sdk/types"
	"github.com/collibra/data-access-go-sdk/utils"
)

type AccessRequestServiceTestSuite struct {
	suite.Suite

	sdkClient           *sdk.CollibraClient
	accessRequestClient *services.AccessRequestClient
	createdDataSource   *schema.DataSource
	createdUser         *schema.User
	dataObjectId        string
}

func TestAccessRequestServiceTestSuite(t *testing.T) {
	suite.Run(t, new(AccessRequestServiceTestSuite))
}

func (suite *AccessRequestServiceTestSuite) SetupSuite() {
	url, clientOptions := utils.GetEnvConfig(&suite.Suite)
	sdkClient, err := sdk.NewClient(url, clientOptions...)
	suite.Require().NoError(err)
	suite.sdkClient = sdkClient

	suite.accessRequestClient = sdkClient.AccessRequest()
	suite.Require().NotNil(suite.accessRequestClient, "Failed to create AccessRequest client")

	dataSourceClient := sdkClient.DataSource()
	suite.Require().NotNil(dataSourceClient, "Failed to create Data Source client")

	dataSource := createDataSource(&suite.Suite, dataSourceClient, nil)
	dataSourceWithMetaData := setDataSourceMetadata(&suite.Suite, dataSourceClient, dataSource.Id, nil)
	suite.createdDataSource = dataSourceWithMetaData

	importDataObjects(&suite.Suite, sdkClient, suite.createdDataSource.Id)

	dataObjectId, err := sdkClient.DataObject().GetDataObjectIdByName(suite.T().Context(), "RAITO_DBT.DEFAULT.CUSTOMER", suite.createdDataSource.Id)
	suite.Require().NoError(err, "Failed to get data object id by name")
	suite.dataObjectId = dataObjectId

	userName := "Test User " + uuid.NewString()
	userEmail := "test.user+" + uuid.NewString() + "@example.com"
	userType := schema.UserTypeHuman
	user, err := sdkClient.User().CreateUser(suite.T().Context(), schema.UserInput{
		Name:  &userName,
		Email: &userEmail,
		Type:  &userType,
	})
	suite.Require().NoError(err, "Failed to create test user")
	suite.Require().NotNil(user, "Created user is nil")
	suite.createdUser = user
}

func (suite *AccessRequestServiceTestSuite) TearDownSuite() {
	ctx := suite.T().Context()
	err := suite.sdkClient.DataSource().DeleteDataSource(ctx, suite.createdDataSource.Id)
	suite.NoError(err, "Failed to delete data source")
}

func (suite *AccessRequestServiceTestSuite) TestCreateAccessRequest() {
	ctx := suite.T().Context()

	name := "Test Access Request " + uuid.NewString()
	description := "Test Access Request Description"
	input := types.AccessRequestInput{
		Name:        &name,
		Description: &description,
		Who: &types.AccessRequestWhoInput{
			Users: []string{suite.createdUser.Id},
		},
		What: []types.AccessRequestWhatInput{
			{
				DataObject: &types.AccessRequestDataObjectWhatInput{
					Id:          suite.dataObjectId,
					Permissions: []string{"SELECT"},
				},
			},
		},
	}

	accessRequest, err := suite.accessRequestClient.CreateAccessRequest(ctx, input)
	suite.Require().NoError(err, "Failed to create AccessRequest")
	suite.Require().NotNil(accessRequest, "Created AccessRequest is nil")
	suite.NotEmpty(accessRequest.Id, "AccessRequest id is empty")
	suite.Require().NotNil(accessRequest.Name, "AccessRequest name is nil")
	suite.Equal(name, *accessRequest.Name)
	suite.Equal(description, accessRequest.Description)
}
