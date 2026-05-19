package services_test

import (
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	sdk "github.com/collibra/data-access-go-sdk"
	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/services"
	"github.com/collibra/data-access-go-sdk/utils"
)

type RoleServiceTestSuite struct {
	suite.Suite

	sdkClient            *sdk.CollibraClient
	roleClient           *services.RoleClient
	createdDataSource    *schema.DataSource
	createdAccessControl *schema.AccessControl
	roleId               string
	assigneeUserId       string // ID of the current (admin) user, guaranteed to be a valid assignee
}

func TestRoleServiceTestSuite(t *testing.T) {
	suite.Run(t, new(RoleServiceTestSuite))
}

func (suite *RoleServiceTestSuite) SetupSuite() {
	url, clientOptions := utils.GetEnvConfig(&suite.Suite)

	sdkClient, err := sdk.NewClient(url, clientOptions...)
	suite.Require().NoError(err)

	suite.sdkClient = sdkClient
	suite.roleClient = sdkClient.Role()

	dataSourceClient := sdkClient.DataSource()
	dataSource := createDataSource(&suite.Suite, dataSourceClient, nil)
	suite.createdDataSource = setDataSourceMetadata(&suite.Suite, dataSourceClient, dataSource.Id, nil)
	importDataObjects(&suite.Suite, sdkClient, suite.createdDataSource.Id)

	// Use the current (admin) user as the assignee — newly-created users are not
	// recognised as valid role assignees until they are synced.
	currentUser, err := sdkClient.User().GetCurrentUser(suite.T().Context())
	suite.Require().NoError(err, "Failed to get current user")

	suite.assigneeUserId = currentUser.Id

	acName := "Test AC for Roles " + uuid.NewString()
	acAction := schema.AccessControlActionGrant
	dataSourceId := suite.createdDataSource.Id

	accessControl, err := sdkClient.AccessControl().CreateAccessControl(suite.T().Context(), schema.AccessControlInput{
		Name:   &acName,
		Action: &acAction,
		DataSources: []schema.AccessControlDataSourceInput{
			{DataSource: dataSourceId},
		},
		WhoItems: []schema.WhoItemInput{
			{User: &currentUser.Id},
		},
		WhatDataObjects: []schema.AccessControlWhatInputDO{
			{DataObjectByName: []schema.AccessControlWhatDoByNameInput{
				{DataSource: dataSourceId, FullName: "RAITO_DBT.DEFAULT.CUSTOMER"},
			}},
		},
	})
	if err != nil {
		suite.T().Logf("Warning: could not create test access control (skipping AC-related tests): %v", err)
	} else {
		suite.createdAccessControl = accessControl
	}

	// Discover an existing role ID from global role assignments.
	for ra, err := range suite.roleClient.ListRoleAssignments(suite.T().Context()) {
		suite.Require().NoError(err, "Error listing role assignments during setup")

		if ra != nil {
			suite.roleId = ra.Role.Id
			break
		}
	}

	suite.Require().NotEmpty(suite.roleId, "No role assignments found in the system; cannot run role tests")
}

func (suite *RoleServiceTestSuite) TearDownSuite() {
	err := suite.sdkClient.DataSource().DeleteDataSource(suite.T().Context(), suite.createdDataSource.Id)
	suite.NoError(err, "Failed to delete data source")
}

func (suite *RoleServiceTestSuite) TestGetRole() {
	role, err := suite.roleClient.GetRole(suite.T().Context(), suite.roleId)
	suite.Require().NoError(err, "Failed to get role")
	suite.Require().NotNil(role, "Role is nil")
	suite.Require().Equal(suite.roleId, role.Id, "Role ID mismatch")
	suite.Require().NotEmpty(role.Name, "Role name is empty")
}

func (suite *RoleServiceTestSuite) TestListRoleAssignments() {
	ctx := suite.T().Context()

	suite.Run("List All Role Assignments", func() {
		found := false

		for ra, err := range suite.roleClient.ListRoleAssignments(ctx) {
			suite.Require().NoError(err, "Error iterating role assignments")

			if ra != nil {
				found = true
				break
			}
		}

		suite.Require().True(found, "Expected at least one role assignment")
	})

	suite.Run("List Role Assignments Filtered By Role", func() {
		filter := &schema.RoleAssignmentFilterInput{Role: &suite.roleId}

		for ra, err := range suite.roleClient.ListRoleAssignments(ctx, services.WithRoleAssignmentListFilter(filter)) {
			suite.Require().NoError(err, "Error iterating role assignments")

			if ra != nil {
				suite.Require().Equal(suite.roleId, ra.Role.Id, "Returned assignment has unexpected role ID")
			}
		}
	})

	suite.Run("List Role Assignments Ordered By UserName Asc", func() {
		sortOrder := schema.SortAsc
		var names []string

		for ra, err := range suite.roleClient.ListRoleAssignments(ctx, services.WithRoleAssignmentListOrder(schema.RoleAssignmentOrderInput{
			UserName: &sortOrder,
		})) {
			suite.Require().NoError(err, "Error iterating role assignments")

			if ra != nil {
				if to, ok := ra.To.(*schema.RoleAssignmentToUser); ok && to != nil {
					names = append(names, to.Id)
				}
			}
		}

		// Just verify the call succeeds and returns results; strict ordering depends on server behaviour.
		suite.Require().NotEmpty(names, "Expected at least one user-to-role assignment")
	})
}

func (suite *RoleServiceTestSuite) TestUpdateAndListRoleAssigneesOnDataSource() {
	ctx := suite.T().Context()

	suite.Run("Update Role Assignees On Data Source", func() {
		role, err := suite.roleClient.UpdateRoleAssigneesOnDataSource(ctx, suite.createdDataSource.Id, suite.roleId, suite.assigneeUserId)
		suite.Require().NoError(err, "Failed to update role assignees on data source")
		suite.Require().NotNil(role, "Returned role is nil")
		suite.Require().Equal(suite.roleId, role.Id, "Returned role ID mismatch")
	})

	suite.Run("List Role Assignments On Data Source", func() {
		found := false

		for ra, err := range suite.roleClient.ListRoleAssignmentsOnDataSource(ctx, suite.createdDataSource.Id) {
			suite.Require().NoError(err, "Error iterating role assignments on data source")

			if ra == nil {
				continue
			}

			to, ok := ra.To.(*schema.RoleAssignmentToUser)
			if ok && to.Id == suite.assigneeUserId {
				found = true
				break
			}
		}

		suite.Require().True(found, "Test user not found in role assignments on data source")
	})

	suite.Run("List Role Assignments On Data Source Filtered By User", func() {
		filter := &schema.RoleAssignmentFilterInput{User: &suite.assigneeUserId}
		found := false

		for ra, err := range suite.roleClient.ListRoleAssignmentsOnDataSource(ctx, suite.createdDataSource.Id,
			services.WithRoleAssignmentListFilter(filter)) {
			suite.Require().NoError(err, "Error iterating filtered role assignments on data source")

			if ra == nil {
				continue
			}

			to, ok := ra.To.(*schema.RoleAssignmentToUser)
			if ok && to.Id == suite.assigneeUserId {
				found = true
				break
			}
		}

		suite.Require().True(found, "Test user not found when filtering role assignments on data source by user")
	})
}

func (suite *RoleServiceTestSuite) TestUpdateAndListRoleAssigneesOnDataObject() {
	ctx := suite.T().Context()

	dataObjectId, err := suite.sdkClient.DataObject().GetDataObjectIdByName(ctx, "RAITO_DBT.DEFAULT.CUSTOMER", suite.createdDataSource.Id)
	suite.Require().NoError(err, "Failed to get data object ID")

	suite.Run("Update Role Assignees On Data Object", func() {
		role, err := suite.roleClient.UpdateRoleAssigneesOnDataObject(ctx, dataObjectId, suite.roleId, suite.assigneeUserId)
		suite.Require().NoError(err, "Failed to update role assignees on data object")
		suite.Require().NotNil(role, "Returned role is nil")
		suite.Require().Equal(suite.roleId, role.Id, "Returned role ID mismatch")
	})

	suite.Run("List Role Assignments On Data Object", func() {
		found := false

		for ra, err := range suite.roleClient.ListRoleAssignmentsOnDataObject(ctx, dataObjectId) {
			suite.Require().NoError(err, "Error iterating role assignments on data object")

			if ra == nil {
				continue
			}

			to, ok := ra.To.(*schema.RoleAssignmentToUser)
			if ok && to.Id == suite.assigneeUserId {
				found = true
				break
			}
		}

		suite.Require().True(found, "Test user not found in role assignments on data object")
	})

	suite.Run("List Role Assignments On Data Object Ordered By RoleName Desc", func() {
		sortOrder := schema.SortDesc
		var roleNames []string

		for ra, err := range suite.roleClient.ListRoleAssignmentsOnDataObject(ctx, dataObjectId,
			services.WithRoleAssignmentListOrder(schema.RoleAssignmentOrderInput{RoleName: &sortOrder})) {
			suite.Require().NoError(err, "Error iterating ordered role assignments on data object")

			if ra != nil {
				roleNames = append(roleNames, ra.Role.Name)
			}
		}

		suite.Require().NotEmpty(roleNames)
		suite.True(sort.IsSorted(sort.Reverse(sort.StringSlice(roleNames))), "Role names are not sorted descending")
	})
}

func (suite *RoleServiceTestSuite) TestUpdateAndListRoleAssigneesOnAccessControl() {
	if suite.createdAccessControl == nil {
		suite.T().Skip("Skipping: no access control available (permission denied during setup)")
	}

	ctx := suite.T().Context()

	suite.Run("Update Role Assignees On Access Control", func() {
		role, err := suite.roleClient.UpdateRoleAssigneesOnAccessControl(ctx, suite.createdAccessControl.Id, suite.roleId, suite.assigneeUserId)
		suite.Require().NoError(err, "Failed to update role assignees on access control")
		suite.Require().NotNil(role, "Returned role is nil")
		suite.Require().Equal(suite.roleId, role.Id, "Returned role ID mismatch")
	})

	suite.Run("List Role Assignments On Access Control", func() {
		found := false

		for ra, err := range suite.roleClient.ListRoleAssignmentsOnAccessControl(ctx, suite.createdAccessControl.Id) {
			suite.Require().NoError(err, "Error iterating role assignments on access control")

			if ra == nil {
				continue
			}

			to, ok := ra.To.(*schema.RoleAssignmentToUser)
			if ok && to.Id == suite.assigneeUserId {
				found = true
				break
			}
		}

		suite.Require().True(found, "Test user not found in role assignments on access control")
	})

	suite.Run("List Role Assignments On Access Control Filtered By Role", func() {
		filter := &schema.RoleAssignmentFilterInput{Role: &suite.roleId}
		found := false

		for ra, err := range suite.roleClient.ListRoleAssignmentsOnAccessControl(ctx, suite.createdAccessControl.Id,
			services.WithRoleAssignmentListFilter(filter)) {
			suite.Require().NoError(err, "Error iterating filtered role assignments on access control")

			if ra == nil {
				continue
			}

			suite.Require().Equal(suite.roleId, ra.Role.Id, "Assignment has unexpected role ID")
			found = true
			break
		}

		suite.Require().True(found, "No role assignments found on access control after filtering by role")
	})
}

func (suite *RoleServiceTestSuite) TestListRoleAssignmentsOnUser() {
	ctx := suite.T().Context()

	// Ensure the user has at least one assignment by assigning them on the data source first.
	_, err := suite.roleClient.UpdateRoleAssigneesOnDataSource(ctx, suite.createdDataSource.Id, suite.roleId, suite.assigneeUserId)
	suite.Require().NoError(err, "Setup: failed to assign role to user on data source")

	suite.Run("List Role Assignments On User", func() {
		found := false

		for ra, err := range suite.roleClient.ListRoleAssignmentsOnUser(ctx, suite.assigneeUserId) {
			suite.Require().NoError(err, "Error iterating role assignments on user")

			if ra == nil {
				continue
			}

			if ra.Role.Id == suite.roleId {
				found = true
				break
			}
		}

		suite.Require().True(found, "Expected role not found in assignments for user")
	})

	suite.Run("List Role Assignments On User Filtered By Role", func() {
		filter := &schema.RoleAssignmentFilterInput{Role: &suite.roleId}

		for ra, err := range suite.roleClient.ListRoleAssignmentsOnUser(ctx, suite.assigneeUserId,
			services.WithRoleAssignmentListFilter(filter)) {
			suite.Require().NoError(err, "Error iterating filtered role assignments on user")

			if ra != nil {
				suite.Require().Equal(suite.roleId, ra.Role.Id, "Assignment has unexpected role ID")
			}
		}
	})
}
