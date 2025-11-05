package services_test

import (
	"encoding/json"
	"os"
	"testing"

	sdk "github.com/collibra/data-access-go-sdk"
	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/services"
	"github.com/collibra/data-access-go-sdk/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

type AccessControlServiceTestSuite struct {
	suite.Suite

	sdkClient            *sdk.CollibraClient
	accessControlClient  *services.AccessControlClient
	createdAccessControl *schema.AccessControl
	createdDataSource    *schema.DataSource
	createdUser          *schema.User
}

func (suite *AccessControlServiceTestSuite) SetupSuite() {
	config := utils.GetEnvConfig(&suite.Suite)
	sdkClient := sdk.NewClient(
		config.User,
		config.Password,
		config.URL,
	)

	suite.Require().NotNil(sdkClient, "Failed to create SDK client")
	suite.sdkClient = sdkClient

	suite.accessControlClient = sdkClient.AccessControl()
	suite.Require().NotNil(suite.accessControlClient, "Failed to create Access Control client")
	// create data source
	dataSourceClient := sdkClient.DataSource()
	suite.Require().NotNil(dataSourceClient, "Failed to create Data Source client")

	suite.createdDataSource = createDataSource(&suite.Suite, dataSourceClient)
	// import data objects
	importWhat(suite)
	// create test user
	userName := "Test User " + uuid.NewString()
	userEmail := "test.user+" + uuid.NewString() + "@example.com"
	userType := schema.UserTypeHuman
	user, err := suite.sdkClient.User().CreateUser(suite.T().Context(), schema.UserInput{
		Name:  &userName,
		Email: &userEmail,
		Type:  &userType,
	})
	suite.Require().NoError(err, "Failed to create test user")
	suite.Require().NotNil(user, "Created user is nil")
	suite.createdUser = user
}

func importWhat(suite *AccessControlServiceTestSuite) {
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

func (suite *AccessControlServiceTestSuite) TearDownSuite() {
	ctx := suite.T().Context()
	err := suite.sdkClient.DataSource().DeleteDataSource(ctx, suite.createdDataSource.Id)
	suite.NoError(err, "Failed to delete data source")
}

func (suite *AccessControlServiceTestSuite) TestA_CreateAccessControl() {
	ctx := suite.T().Context()
	name := "Test Access Control " + uuid.New().String()
	action := schema.AccessControlActionGrant
	user := suite.createdUser.Id
	dataSource := suite.createdDataSource.Id
	fullName := "RAITO_DBT"
	whatDataObjects := schema.AccessControlWhatInputDO{
		DataObjectByName: []schema.AccessControlWhatDoByNameInput{
			{
				DataSource: dataSource,
				FullName:   fullName,
			},
		},
	}
	accessControl, err := suite.accessControlClient.CreateAccessControl(ctx, schema.AccessControlInput{
		Name:   &name,
		Action: &action,
		DataSources: []schema.AccessControlDataSourceInput{
			{DataSource: dataSource},
		},
		WhoItems: []schema.WhoItemInput{
			{User: &user},
		},
		WhatDataObjects: []schema.AccessControlWhatInputDO{whatDataObjects},
	})
	suite.Require().NoError(err)
	suite.Require().NotNil(accessControl)
	suite.Require().Equal(name, accessControl.Name)
	suite.Require().Equal(action, accessControl.Action)
	suite.Require().Equal(dataSource, accessControl.SyncData[0].DataSource.DataSource.Id)
	// store created access control for further tests
	suite.createdAccessControl = accessControl
}

func (suite *AccessControlServiceTestSuite) TestB_ListAccessControlsWithFilterByDataSource() {
	createdAccessControl := suite.createdAccessControl
	suite.Require().NotNil(createdAccessControl, "Created access control is nil")
	ctx := suite.T().Context()
	client := suite.sdkClient.AccessControl()
	filter := &schema.AccessControlFilterInput{
		DataSource: &suite.createdDataSource.Id,
	}
	found := false
	response := client.ListAccessControls(ctx, services.WithAccessControlListFilter(filter))
	for accessControl, err := range response {
		suite.Require().NoError(err, "Error listing access controls")
		if accessControl.Id == createdAccessControl.Id {
			suite.Equal(createdAccessControl.Name, accessControl.Name)
			suite.Equal(createdAccessControl.Action, accessControl.Action)
			found = true
			return
		}
	}
	suite.Require().True(found, "Created access control not found in list")
}

func (suite *AccessControlServiceTestSuite) TestC_UpdateAccessControl() {
	createdAccessControl := suite.createdAccessControl
	suite.Require().NotNil(createdAccessControl, "Created access control is nil")
	ctx := suite.T().Context()
	client := suite.sdkClient.AccessControl()
	newName := "Updated Access Control Name " + uuid.New().String()
	updatedAccessControl, err := client.UpdateAccessControl(ctx, createdAccessControl.Id, schema.AccessControlInput{
		Name: &newName,
	})
	suite.Require().NoError(err, "Failed to update access control")
	suite.Require().NotNil(updatedAccessControl, "Updated access control is nil")
	suite.Require().Equal(newName, updatedAccessControl.Name, "Access control name was not updated")
	suite.createdAccessControl = updatedAccessControl
}

func (suite *AccessControlServiceTestSuite) TestD_DeactivateAccessControl() {
	createdAccessControl := suite.createdAccessControl
	suite.Require().NotNil(createdAccessControl, "Created access control is nil")
	ctx := suite.T().Context()
	client := suite.sdkClient.AccessControl()
	// deactivate access control. Test assumes that created access control is active
	_, err := client.DeactivateAccessControl(ctx, createdAccessControl.Id)
	suite.Require().NoError(err, "Failed to deactivate access control")
	// get and verify deactivation
	deactivatedAccessControl, err := client.GetAccessControl(ctx, createdAccessControl.Id)
	suite.Require().NoError(err, "Failed to get access control after deactivation")
	suite.Require().NotNil(deactivatedAccessControl, "Deactivated access control is nil")
	suite.Require().Equal(deactivatedAccessControl.State, schema.AccessControlStateInactive, "Access control was not deactivated") 
	suite.createdAccessControl = deactivatedAccessControl
}

func (suite *AccessControlServiceTestSuite) TestE_ActivateAccessControl() {
	createdAccessControl := suite.createdAccessControl
	suite.Require().NotNil(createdAccessControl, "Created access control is nil")
	ctx := suite.T().Context()
	client := suite.sdkClient.AccessControl()
	// activate access control back
	_, err := client.ActivateAccessControl(ctx, createdAccessControl.Id)
	suite.Require().NoError(err, "Failed to activate access control")
	// get and verify activation
	activatedAccessControl, err := client.GetAccessControl(ctx, createdAccessControl.Id)
	suite.Require().NoError(err, "Failed to get access control after activation")
	suite.Require().NotNil(activatedAccessControl, "Activated access control is nil")
	suite.Require().Equal(activatedAccessControl.State, schema.AccessControlStateActive, "Access control was not activated") 
	suite.createdAccessControl = activatedAccessControl
}

func (suite *AccessControlServiceTestSuite) TestF_GetAccessControlWhoListWithAscSortOrder() {
	createdAccessControl := suite.createdAccessControl
	suite.Require().NotNil(createdAccessControl, "Created access control is nil")
	createdUser := suite.createdUser
	suite.Require().NotNil(createdUser, "Created user is nil")
	ctx := suite.T().Context()
	client := suite.sdkClient.AccessControl()
	sortOrder := schema.SortAsc
	response := client.GetAccessControlWhoList(ctx, createdAccessControl.Id, services.WithAccessControlWhoListOrder(schema.AccessControlWhoOrderByInput{
		Name: &sortOrder,
	}))
	found := false
	for who, err := range response {
		suite.Require().NoError(err, "Error listing access control who items")
		item := who.GetItem()
		typename := item.GetTypename()
		expectedUserType := "User"
		// Type assert to access the Email field and compare
		if typename != nil && *typename == expectedUserType {
			user := item.(*schema.AccessWhoItemItemUser)
			email := user.Email
			expectedEmail := createdUser.Email
			if email != nil && expectedEmail != nil && *email == *expectedEmail {
				found = true
				return
			}
		}
	}
	suite.Require().True(found, "Expected user %s not found in access control who list", createdUser.Email)
}

func (suite *AccessControlServiceTestSuite) TestG_GetAccessControlWhatDataObjectListWithDescOrder() {
	ctx := suite.T().Context()
	client := suite.sdkClient.AccessControl()
	sortOrder := schema.SortDesc
	response := client.GetAccessControlWhatDataObjectList(ctx, suite.createdAccessControl.Id, services.WithAccessControlWhatListOrder(schema.AccessWhatOrderByInput{
		Name: &sortOrder,
	}))

	found := false
	for what, err := range response {
		suite.Require().NoError(err, "Error listing access control what data objects")
		if what.DataObject.FullName == "RAITO_DBT" {
			found = true
			return
		}
	}
	suite.Require().True(found, "Expected data object RAITO_DBT not found in access control what list")
}

func (suite *AccessControlServiceTestSuite) TestH_GetAccessControlABACWhatScope() {
	createdAccessControl := suite.createdAccessControl
	suite.Require().NotNil(createdAccessControl, "Created access control is nil")
	createdUser := suite.createdUser
	suite.Require().NotNil(createdUser, "Created user is nil")

	ctx := suite.T().Context()
	client := suite.sdkClient.AccessControl()
	name := "Test Access Control for WhatAccessControlList " + uuid.New().String()
	action := schema.AccessControlActionGrant
	dataSource := suite.createdDataSource.Id
	whatType := schema.WhoAndWhatTypeDynamic

	stringLiteral := "Stripe"
	// TODO: adjust data here so the GetAccessControlAbacWhatScope response contains at least one item
	whatAbacRule := schema.WhatAbacRuleInput{
		DoTypes: []string{"schema"},
		Permissions: []string{"READ"},
		Rule: schema.AbacComparisonExpressionInput{
			Comparison: &schema.AbacComparisonExpressionComparisonInput{Operator: schema.AbacComparisonExpressionComparisonOperatorHastag,
			LeftOperand: "source_system",
			RightOperand: schema.AbacComparisonExpressionOperandInput{
				Literal: &schema.AbacComparisonExpressionLiteral{
					String: &stringLiteral,
				},
			}},
			
		},
	}

	accessControl, err := client.CreateAccessControl(ctx, schema.AccessControlInput{
		Name:   &name,
		Action: &action,
		DataSources: []schema.AccessControlDataSourceInput{
			{DataSource: dataSource},
		},
		WhatType: &whatType,
		WhoItems: []schema.WhoItemInput{
			{User: &createdUser.Id},
		},
		WhatAbacRule: &whatAbacRule,
	})
	suite.Require().NoError(err, "Failed to create access control with ABAC what scope") 
	suite.Require().NotNil(accessControl, "Created access control is nil")

	// get WhatAbacRuleList for the created access control and verify that the rule is listed there
	response := client.GetAccessControlAbacWhatScope(ctx, accessControl.Id) // Empty list here
	found := false
	for item, err := range response {
		// TODO: implement this comparison properly
		suite.Require().NoError(err, "Error listing access control what ABAC rules")
		if item != nil {
			found = true
			break
		}
	}
	suite.Require().True(found, "Expected ABAC what scope not found in access control")
}

func (suite *AccessControlServiceTestSuite) TestI_GetAccessControlWhatAccessControlList() {
	createdAccessControl := suite.createdAccessControl
	suite.Require().NotNil(createdAccessControl, "Created access control is nil")
	ctx := suite.T().Context()
	client := suite.sdkClient.AccessControl()
	// create another access control that references the first one in WhatAccessControlList
	name := "Test Access Control for WhatAccessControlList " + uuid.New().String()
	action := schema.AccessControlActionGrant
	dataSource := suite.createdDataSource.Id
	fullName := "RAITO_DBT.DEFAULT.CUSTOMER"
	whatDataObjects := schema.AccessControlWhatInputDO{
		DataObjectByName: []schema.AccessControlWhatDoByNameInput{
			{
				DataSource: dataSource,
				FullName:   fullName,
			},
		},
	}

	accessControl, err := suite.accessControlClient.CreateAccessControl(ctx, schema.AccessControlInput{
		Name:   &name,
		Action: &action,
		DataSources: []schema.AccessControlDataSourceInput{
			{DataSource: dataSource},
		},
		WhoItems: []schema.WhoItemInput{
			{AccessControl: &createdAccessControl.Id},
		},
		WhatDataObjects: []schema.AccessControlWhatInputDO{whatDataObjects},
	})
	suite.Require().NoError(err, "Failed to create access control referencing another one")
	suite.Require().NotNil(accessControl, "Created access control is nil")

	// get WhatAccessControlList for the first created access control and verify that the second one is listed there
	response := client.GetAccessControlWhatAccessControlList(ctx, createdAccessControl.Id)
	found := false
	for item, err := range response {
		// todo: create a better comparison and check properly the access control IDs
		suite.Require().NoError(err, "Error listing access control what data objects")
		if item.AccessControl.Id == accessControl.Id {
			found = true
			return
		}
	}
	suite.Require().True(found, "Expected access control not found in what access control list")
}

func (suite *AccessControlServiceTestSuite) TestJ_DeleteAccessControl() {
	createdAccessControl := suite.createdAccessControl
	suite.Require().NotNil(createdAccessControl, "Created access control is nil")
	ctx := suite.T().Context()
	client := suite.sdkClient.AccessControl()

	err := client.DeleteAccessControl(ctx, createdAccessControl.Id)
	suite.Require().NoError(err, "Failed to delete access control")

	// try to get deleted access control
	deletedAccessControl, err := client.GetAccessControl(ctx, createdAccessControl.Id)
	suite.Require().NoError(err, "Expected error when getting deleted access control")	
	suite.Require().Equal(deletedAccessControl.State, schema.AccessControlStateDeleted, "Deleted access control should have a 'Deleted' state")
}

func TestAccessControlServiceTestSuite(t *testing.T) {
	suite.Run(t, new(AccessControlServiceTestSuite))
}
