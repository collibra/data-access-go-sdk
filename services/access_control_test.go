package services_test

import (
	"context"
	"sort"
	"testing"

	sdk "github.com/collibra/data-access-go-sdk"
	"github.com/collibra/data-access-go-sdk/internal"
	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/services"
	"github.com/collibra/data-access-go-sdk/types"
	"github.com/collibra/data-access-go-sdk/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type AccessControlServiceTestSuite struct {
	suite.Suite

	sdkClient         *sdk.CollibraClient
	createdDataSource *schema.DataSource
	createdUser       *schema.User
}

func createTestAccessControl(suite *AccessControlServiceTestSuite, accessControlClient *services.AccessControlClient, createdUser *schema.User, name *string, action *schema.AccessControlAction, dataSourceId *string) (*schema.AccessControl, error) {
	ctx := suite.T().Context()

	fullNames := []string{"RAITO_DBT.DEFAULT.CUSTOMER.FIRSTNAME", "RAITO_DBT.DEFAULT.CUSTOMER.LASTNAME"}
	whatDataObjects := schema.AccessControlWhatInputDO{
		DataObjectByName: []schema.AccessControlWhatDoByNameInput{
			{
				DataSource: *dataSourceId,
				FullName:   fullNames[0],
			},
			{
				DataSource: *dataSourceId,
				FullName:   fullNames[1],
			},
		},
	}
	accessControl, err := accessControlClient.CreateAccessControl(ctx, schema.AccessControlInput{
		Name:   name,
		Action: action,
		DataSources: []schema.AccessControlDataSourceInput{
			{DataSource: *dataSourceId},
		},
		WhoItems: []schema.WhoItemInput{
			{User: &createdUser.Id},
		},
		WhatDataObjects: []schema.AccessControlWhatInputDO{whatDataObjects},
	})

	return accessControl, err
}

func TestAccessControlServiceTestSuite(t *testing.T) {
	suite.Run(t, new(AccessControlServiceTestSuite))
}

func (suite *AccessControlServiceTestSuite) SetupSuite() {
	url, clientOptions := utils.GetEnvConfig(&suite.Suite)
	sdkClient, err := sdk.NewClient(url, clientOptions...)
	suite.Require().NoError(err)
	suite.sdkClient = sdkClient
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
	user, err := sdkClient.User().CreateUser(suite.T().Context(), schema.UserInput{
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

func (suite *AccessControlServiceTestSuite) TestAccessControls() {
	ctx := suite.T().Context()
	accessControlClient := suite.sdkClient.AccessControl()
	var createdAccessControl *schema.AccessControl
	createdUser := suite.createdUser

	suite.Run("Create Access Control", func() {
		name := "Test Access Control " + uuid.New().String()
		action := schema.AccessControlActionGrant
		accessControl, err := createTestAccessControl(suite, accessControlClient, createdUser, &name, &action, &suite.createdDataSource.Id)
		suite.Require().NoError(err)
		suite.Require().NotNil(accessControl)
		suite.Require().Equal(name, accessControl.Name)
		suite.Require().Equal(action, accessControl.Action)
		suite.Require().Equal(suite.createdDataSource.Id, accessControl.SyncData[0].DataSource.Id)
		// store created access control for further tests
		createdAccessControl = accessControl
	})

	suite.Run("List Access Controls With Filter By Data Source", func() {
		if createdAccessControl == nil {
			suite.T().Skip("Skipping test because createdAccessControl is nil")
		}

		filter := &schema.AccessControlFilterInput{
			DataSource: &suite.createdDataSource.Id,
		}
		found := false

		response := accessControlClient.ListAccessControls(ctx, services.WithAccessControlListFilter(filter))
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
	})

	suite.Run("Update Access Control", func() {
		if createdAccessControl == nil {
			suite.T().Skip("Skipping test because createdAccessControl is nil")
		}

		newName := "Updated Access Control Name " + uuid.New().String()
		updatedAccessControl, err := accessControlClient.UpdateAccessControl(ctx, createdAccessControl.Id, schema.AccessControlInput{
			Name: &newName,
		})
		suite.Require().NoError(err, "Failed to update access control")
		suite.Require().NotNil(updatedAccessControl, "Updated access control is nil")
		suite.Require().Equal(newName, updatedAccessControl.Name, "Access control name was not updated")
		createdAccessControl = updatedAccessControl
	})

	suite.Run("Deactivate Access Control", func() {
		if createdAccessControl == nil {
			suite.T().Skip("Skipping test because createdAccessControl is nil")
		}
		// deactivate access control. Test assumes that created access control is active
		_, err := accessControlClient.DeactivateAccessControl(ctx, createdAccessControl.Id)
		suite.Require().NoError(err, "Failed to deactivate access control")
		// get and verify deactivation
		deactivatedAccessControl, err := accessControlClient.GetAccessControl(ctx, createdAccessControl.Id)
		suite.Require().NoError(err, "Failed to get access control after deactivation")
		suite.Require().NotNil(deactivatedAccessControl, "Deactivated access control is nil")
		suite.Require().Equal(schema.AccessControlStateInactive, deactivatedAccessControl.State, "Access control was not deactivated")
		createdAccessControl = deactivatedAccessControl
	})

	suite.Run("Activate Access Control", func() {
		if createdAccessControl == nil {
			suite.T().Skip("Skipping test because createdAccessControl is nil")
		}
		// activate access control back
		_, err := accessControlClient.ActivateAccessControl(ctx, createdAccessControl.Id)
		suite.Require().NoError(err, "Failed to activate access control")
		// get and verify activation
		activatedAccessControl, err := accessControlClient.GetAccessControl(ctx, createdAccessControl.Id)
		suite.Require().NoError(err, "Failed to get access control after activation")
		suite.Require().NotNil(activatedAccessControl, "Activated access control is nil")
		suite.Require().Equal(schema.AccessControlStateActive, activatedAccessControl.State, "Access control was not activated")
		createdAccessControl = activatedAccessControl
	})

	suite.Run("Delete Access Control", func() {
		if createdAccessControl == nil {
			suite.T().Skip("Skipping test because createdAccessControl is nil")
		}

		err := accessControlClient.DeleteAccessControl(ctx, createdAccessControl.Id)
		suite.Require().NoError(err, "Failed to delete access control")

		// try to get deleted access control
		deletedAccessControl, err := accessControlClient.GetAccessControl(ctx, createdAccessControl.Id)
		suite.Require().NoError(err, "Expected error when getting deleted access control")
		suite.Require().Equal(schema.AccessControlStateDeleted, deletedAccessControl.State, "Deleted access control should have a 'Deleted' state")
	})
}

func (suite *AccessControlServiceTestSuite) TestDeleteUnexistentAccessControl() {
	ctx := suite.T().Context()
	client := suite.sdkClient.AccessControl()
	nonExistentAccessControlId := uuid.NewString()

	err := client.DeleteAccessControl(ctx, nonExistentAccessControlId)
	suite.Require().Error(err, "Expected error when deleting non-existent access control")
	suite.Require().Contains(err.Error(), "not found", "Error message should indicate that access control was not found")
}

func (suite *AccessControlServiceTestSuite) TestAccessControlsGetWhoList() {
	ctx := suite.T().Context()
	accessControlClient := suite.sdkClient.AccessControl()
	name := "Test Access Control " + uuid.New().String()
	action := schema.AccessControlActionGrant
	createdAccessControl, err := createTestAccessControl(suite, accessControlClient, suite.createdUser, &name, &action, &suite.createdDataSource.Id)
	suite.Require().NoError(err)

	createdUser := suite.createdUser

	suite.Run("Get Access Control Who List", func() {
		if createdAccessControl == nil || createdUser == nil {
			suite.T().Skip("Skipping test because createdAccessControl or createdUser is nil")
		}

		response := accessControlClient.GetAccessControlWhoList(ctx, createdAccessControl.Id)
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
	})

	suite.Run("Get Access Control Who List With Asc Sort Order", func() {
		if createdAccessControl == nil || createdUser == nil {
			suite.T().Skip("Skipping test because createdAccessControl or createdUser is nil")
		}

		sortOrder := schema.SortAsc
		response := accessControlClient.GetAccessControlWhoList(ctx, createdAccessControl.Id, services.WithAccessControlWhoListOrder(schema.AccessControlWhoOrderByInput{
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
	})
}

func (suite *AccessControlServiceTestSuite) TestAccessControlsGetWhatDataObjectList() {
	ctx := suite.T().Context()
	accessControlClient := suite.sdkClient.AccessControl()
	name := "Test Access Control " + uuid.New().String()
	action := schema.AccessControlActionGrant
	createdAccessControl, err := createTestAccessControl(suite, accessControlClient, suite.createdUser, &name, &action, &suite.createdDataSource.Id)
	suite.Require().NoError(err)

	suite.Run("Get Access Control What Data Object List", func() {
		if createdAccessControl == nil {
			suite.T().Skip("Skipping test because createdAccessControl is nil")
		}

		response := accessControlClient.GetAccessControlWhatDataObjectList(ctx, createdAccessControl.Id)
		found := false

		for what, err := range response {
			suite.Require().NoError(err, "Error listing access control what data objects")

			if what.DataObject.FullName == "RAITO_DBT.DEFAULT.CUSTOMER.LASTNAME" {
				found = true
				break
			}
		}

		suite.Require().True(found, "Expected data object RAITO_DBT.DEFAULT.CUSTOMER.LASTNAME not found in access control what list")
	})

	suite.Run("Get Access Control What Data Object List With Desc Sort Order", func() {
		if createdAccessControl == nil {
			suite.T().Skip("Skipping test because createdAccessControl is nil")
		}

		sortOrder := schema.SortDesc
		response := accessControlClient.GetAccessControlWhatDataObjectList(ctx, createdAccessControl.Id, services.WithAccessControlWhatListOrder(schema.AccessWhatOrderByInput{
			Name: &sortOrder,
		}))

		availableItems := []string{}

		for what, err := range response {
			suite.Require().NoError(err, "Error listing access control what data objects")

			availableItems = append(availableItems, what.DataObject.FullName)
		}

		suite.Require().Greater(len(availableItems), 1, "Not enough items to verify descending order")
		suite.True(sort.IsSorted(sort.Reverse(sort.StringSlice(availableItems))), "Data source names are not sorted in descending order")
	})
}

func (suite *AccessControlServiceTestSuite) TestAccessControlsGetWhatAccessControlList() {
	ctx := suite.T().Context()
	accessControlClient := suite.sdkClient.AccessControl()
	name := "Test Access Control " + uuid.New().String()
	action := schema.AccessControlActionGrant
	createdAccessControl, err := createTestAccessControl(suite, accessControlClient, suite.createdUser, &name, &action, &suite.createdDataSource.Id)
	suite.Require().NoError(err)

	suite.Run("Get Access Control What Access Control List", func() {
		if createdAccessControl == nil {
			suite.T().Skip("Skipping test because createdAccessControl is nil")
		}
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

		accessControlThanRefersExistingOne, err := accessControlClient.CreateAccessControl(ctx, schema.AccessControlInput{
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

		response := accessControlClient.GetAccessControlWhatAccessControlList(ctx, createdAccessControl.Id)
		found := false

		for item, err := range response {
			suite.Require().NoError(err, "Error listing access control what data objects")

			if item.AccessControl.Id == accessControlThanRefersExistingOne.Id {
				found = true
				break
			}
		}

		suite.Require().True(found, "Expected access control not found in what access control list")
	})
}

func (suite *AccessControlServiceTestSuite) TestGetAccessControlABACWhatScope() {
	suite.T().Skipf("Test not yet ready and not working properly")
	ctx := suite.T().Context()
	accessControlClient := suite.sdkClient.AccessControl()
	createdUser := suite.createdUser

	if createdUser == nil {
		suite.T().Skip("Skipping test because createdUser is nil")
	}

	name := "Test Access Control for WhatAccessControlList " + uuid.New().String()
	action := schema.AccessControlActionGrant
	dataSource := suite.createdDataSource.Id

	stringLiteral := "Stripe"
	// TODO: adjust data here so the GetAccessControlAbacWhatScope response contains at least one item
	whatAbacRule := schema.WhatAbacRuleInput{
		Id:          new("rule1"),
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

	accessControl, err := accessControlClient.CreateAccessControl(ctx, schema.AccessControlInput{
		Name:   &name,
		Action: &action,
		DataSources: []schema.AccessControlDataSourceInput{
			{DataSource: dataSource},
		},
		WhoItems: []schema.WhoItemInput{
			{User: &createdUser.Id},
		},
		WhatAbacRules: []*schema.WhatAbacRuleInput{&whatAbacRule},
	})
	suite.Require().NoError(err, "Failed to create access control with ABAC what scope")
	suite.Require().NotNil(accessControl, "Created access control is nil")

	// get WhatAbacRuleList for the created access control and verify that the rule is listed there
	response := accessControlClient.GetAccessControlAbacWhatScope(ctx, accessControl.Id, "rule1") // Empty list here
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

func TestListAccessControlsPage_PageSizeExceedsMax(t *testing.T) {
	client := services.NewAccessControlClient(nil)
	_, _, err := client.ListAccessControlsPage(context.Background(), services.WithAccessControlListPageSize(internal.MaxPageSize+1))
	require.Error(t, err)
	var invalidInput *types.ErrInvalidInput
	require.ErrorAs(t, err, &invalidInput)
}

func TestListAccessControlsPage_PageSizeAtMax(t *testing.T) {
	// MaxPageSize itself is valid: validation passes and the call reaches the API
	// (which panics on a nil client rather than returning ErrInvalidInput).
	client := services.NewAccessControlClient(nil)

	assert.Panics(t, func() {
		client.ListAccessControlsPage(context.Background(), services.WithAccessControlListPageSize(internal.MaxPageSize)) //nolint:errcheck
	})
}
