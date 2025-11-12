package services_test

import (
	"sort"
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
	accessControlClient := sdkClient.AccessControl()
	suite.accessControlClient = accessControlClient
	suite.Require().NotNil(suite.accessControlClient, "Failed to create Access Control client")
	// create data source
	dataSourceClient := sdkClient.DataSource()
	suite.Require().NotNil(dataSourceClient, "Failed to create Data Source client")

	dataSource := createDataSource(&suite.Suite, dataSourceClient, nil)
	dataSourceWithMetaData := setDataSourceMetadata(&suite.Suite, dataSourceClient, dataSource.Id, nil)

	suite.createdDataSource = dataSourceWithMetaData
	// import data objects
	importDataObjects(&suite.Suite, sdkClient, suite.createdDataSource.Id)
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
	fullNames := []string{"RAITO_DBT.DEFAULT.CUSTOMER.FIRSTNAME", "RAITO_DBT.DEFAULT.CUSTOMER.LASTNAME"}
	whatDataObjects := schema.AccessControlWhatInputDO{
		DataObjectByName: []schema.AccessControlWhatDoByNameInput{
			{
				DataSource: dataSource,
				FullName:   fullNames[0],
			},
			{
				DataSource: dataSource,
				FullName:   fullNames[1],
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
	suite.Require().Equal(dataSource, accessControl.SyncData[0].DataSource.Id)
	// store created access control for further tests
	suite.createdAccessControl = accessControl
}

func (suite *AccessControlServiceTestSuite) TestB_ListAccessControlsWithFilterByDataSource() {
	createdAccessControl := suite.createdAccessControl
	suite.Require().NotNil(createdAccessControl, "Created access control is nil")
	ctx := suite.T().Context()
	client := suite.accessControlClient
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
			break
		}
	}

	suite.Require().True(found, "Created access control not found in list")
}

func (suite *AccessControlServiceTestSuite) TestC_UpdateAccessControl() {
	createdAccessControl := suite.createdAccessControl
	suite.Require().NotNil(createdAccessControl, "Created access control is nil")
	ctx := suite.T().Context()
	client := suite.accessControlClient
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
	client := suite.accessControlClient
	// deactivate access control. Test assumes that created access control is active
	_, err := client.DeactivateAccessControl(ctx, createdAccessControl.Id)
	suite.Require().NoError(err, "Failed to deactivate access control")
	// get and verify deactivation
	deactivatedAccessControl, err := client.GetAccessControl(ctx, createdAccessControl.Id)
	suite.Require().NoError(err, "Failed to get access control after deactivation")
	suite.Require().NotNil(deactivatedAccessControl, "Deactivated access control is nil")
	suite.Require().Equal(schema.AccessControlStateInactive, deactivatedAccessControl.State, "Access control was not deactivated")
	suite.createdAccessControl = deactivatedAccessControl
}

func (suite *AccessControlServiceTestSuite) TestE_ActivateAccessControl() {
	createdAccessControl := suite.createdAccessControl
	suite.Require().NotNil(createdAccessControl, "Created access control is nil")
	ctx := suite.T().Context()
	client := suite.accessControlClient
	// activate access control back
	_, err := client.ActivateAccessControl(ctx, createdAccessControl.Id)
	suite.Require().NoError(err, "Failed to activate access control")
	// get and verify activation
	activatedAccessControl, err := client.GetAccessControl(ctx, createdAccessControl.Id)
	suite.Require().NoError(err, "Failed to get access control after activation")
	suite.Require().NotNil(activatedAccessControl, "Activated access control is nil")
	suite.Require().Equal(schema.AccessControlStateActive, activatedAccessControl.State, "Access control was not activated")
	suite.createdAccessControl = activatedAccessControl
}

func (suite *AccessControlServiceTestSuite) TestF_GetAccessControlWhoList() {
	createdAccessControl := suite.createdAccessControl
	suite.Require().NotNil(createdAccessControl, "Created access control is nil")
	createdUser := suite.createdUser
	suite.Require().NotNil(createdUser, "Created user is nil")
	ctx := suite.T().Context()
	client := suite.accessControlClient

	response := client.GetAccessControlWhoList(ctx, createdAccessControl.Id)
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
				break
			}
		}
	}

	suite.Require().True(found, "Expected user %s not found in access control who list", createdUser.Email)
}

func (suite *AccessControlServiceTestSuite) TestG_GetAccessControlWhoListWithAscSortOrder() {
	createdAccessControl := suite.createdAccessControl
	suite.Require().NotNil(createdAccessControl, "Created access control is nil")
	createdUser := suite.createdUser
	suite.Require().NotNil(createdUser, "Created user is nil")
	ctx := suite.T().Context()
	client := suite.accessControlClient
	sortOrder := schema.SortAsc
	response := client.GetAccessControlWhoList(ctx, createdAccessControl.Id, services.WithAccessControlWhoListOrder(schema.AccessControlWhoOrderByInput{
		Name: &sortOrder,
	}))
	availableNames := []string{}

	for who, err := range response {
		suite.Require().NoError(err, "Error listing access control who items")

		item := who.GetItem()
		typename := item.GetTypename()
		expectedUserType := "User"
		// Type assert to access the Name field and collect names
		if typename != nil && *typename == expectedUserType {
			user := item.(*schema.AccessWhoItemItemUser)
			name := user.Name
			availableNames = append(availableNames, name)
		}
	}

	suite.True(sort.StringsAreSorted(availableNames), "Access control who names are not sorted in ascending order")
}

func (suite *AccessControlServiceTestSuite) TestH_GetAccessControlWhatDataObjectList() {
	ctx := suite.T().Context()
	client := suite.accessControlClient
	response := client.GetAccessControlWhatDataObjectList(ctx, suite.createdAccessControl.Id)
	found := false

	for what, err := range response {
		suite.Require().NoError(err, "Error listing access control what data objects")

		if what.DataObject.FullName == "RAITO_DBT.DEFAULT.CUSTOMER.LASTNAME" {
			found = true
			break
		}
	}

	suite.Require().True(found, "Expected data object RAITO_DBT.DEFAULT.CUSTOMER.LASTNAME not found in access control what list")
}

func (suite *AccessControlServiceTestSuite) TestI_GetAccessControlWhatDataObjectListWithDescOrder() {
	ctx := suite.T().Context()
	client := suite.accessControlClient
	sortOrder := schema.SortDesc
	response := client.GetAccessControlWhatDataObjectList(ctx, suite.createdAccessControl.Id, services.WithAccessControlWhatListOrder(schema.AccessWhatOrderByInput{
		Name: &sortOrder,
	}))

	availableItems := []string{}

	for what, err := range response {
		suite.Require().NoError(err, "Error listing access control what data objects")

		availableItems = append(availableItems, what.DataObject.FullName)
	}

	suite.Require().Greater(len(availableItems), 1, "Not enough items to verify descending order")
	suite.True(sort.IsSorted(sort.Reverse(sort.StringSlice(availableItems))), "Data source names are not sorted in descending order")
}
func (suite *AccessControlServiceTestSuite) TestJ_GetAccessControlABACWhatScope() {
	suite.T().Skipf("Test not yet ready and not working properly")
	createdAccessControl := suite.createdAccessControl
	suite.Require().NotNil(createdAccessControl, "Created access control is nil")
	createdUser := suite.createdUser
	suite.Require().NotNil(createdUser, "Created user is nil")

	ctx := suite.T().Context()
	client := suite.accessControlClient
	name := "Test Access Control for WhatAccessControlList " + uuid.New().String()
	action := schema.AccessControlActionGrant
	dataSource := suite.createdDataSource.Id
	whatType := schema.WhoAndWhatTypeDynamic

	stringLiteral := "Stripe"
	// TODO: adjust data here so the GetAccessControlAbacWhatScope response contains at least one item
	whatAbacRule := schema.WhatAbacRuleInput{
		DoTypes:     []string{"schema"},
		Permissions: []string{"READ"},
		Scope:       []string{"RAITO_DBT"},
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

func (suite *AccessControlServiceTestSuite) TestK_GetAccessControlWhatAccessControlList() {
	createdAccessControl := suite.createdAccessControl
	suite.Require().NotNil(createdAccessControl, "Created access control is nil")
	ctx := suite.T().Context()
	client := suite.accessControlClient
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

	accessControlThanRefersExistingOne, err := suite.accessControlClient.CreateAccessControl(ctx, schema.AccessControlInput{
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
	suite.Require().NotNil(accessControlThanRefersExistingOne, "Created access control is nil")

	response := client.GetAccessControlWhatAccessControlList(ctx, createdAccessControl.Id)
	found := false

	for item, err := range response {
		suite.Require().NoError(err, "Error listing access control what data objects")

		if item.AccessControl.Id == accessControlThanRefersExistingOne.Id {
			found = true
			break
		}
	}

	suite.Require().True(found, "Expected access control not found in what access control list")
}

func (suite *AccessControlServiceTestSuite) TestL_DeleteAccessControl() {
	createdAccessControl := suite.createdAccessControl
	suite.Require().NotNil(createdAccessControl, "Created access control is nil")
	ctx := suite.T().Context()
	client := suite.accessControlClient

	err := client.DeleteAccessControl(ctx, createdAccessControl.Id)
	suite.Require().NoError(err, "Failed to delete access control")

	// try to get deleted access control
	deletedAccessControl, err := client.GetAccessControl(ctx, createdAccessControl.Id)
	suite.Require().NoError(err, "Expected error when getting deleted access control")
	suite.Require().Equal(schema.AccessControlStateDeleted, deletedAccessControl.State, "Deleted access control should have a 'Deleted' state")
}

func TestAccessControlServiceTestSuite(t *testing.T) {
	suite.Run(t, new(AccessControlServiceTestSuite))
}
