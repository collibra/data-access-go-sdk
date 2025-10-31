package services_test

import (
	"testing"

	sdk "github.com/collibra/data-access-go-sdk"
	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/services"
	"github.com/collibra/data-access-go-sdk/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

type GrantCategoryServiceTestSuite struct {
	suite.Suite
	GrantCategoryClient  *services.GrantCategoryClient
	createdGrantCategory *schema.GrantCategoryDetails
}

func (suite *GrantCategoryServiceTestSuite) SetupSuite() {
	config := utils.GetEnvConfig(&suite.Suite)
	client := sdk.NewClient(
		config.User,
		config.Password,
		config.URL,
	)

	if client == nil {
		suite.FailNow("Failed to create Collibra client")
	}
	grantCategoryClient := client.GrantCategory()
	if grantCategoryClient == nil {
		suite.FailNow("Failed to create GrantCategory client")
	}
	suite.GrantCategoryClient = grantCategoryClient
}

func (suite *GrantCategoryServiceTestSuite) TestA_CreateGrantCategory() {
	ctx := suite.T().Context()
	name := "Test Category " + uuid.New().String()
	description := "Test Category Description"
	canCreate := true
	canRequestAccess := true
	allowDuplicateNames := false
	icon := "📁"
	input := &schema.GrantCategoryInput{
		Name:                &name,
		Description:         &description,
		Icon:                &icon,
		CanCreate:           &canCreate,
		CanRequestAccess:    &canRequestAccess,
		AllowDuplicateNames: &allowDuplicateNames,
	}

	createdGrant, err := suite.GrantCategoryClient.CreateGrantCategory(ctx, *input)

	suite.NoError(err, "Failed to create GrantCategory")

	suite.NotNil(createdGrant)
	suite.Equal(name, createdGrant.Name)
	suite.Equal(description, createdGrant.Description)
	suite.Equal(icon, createdGrant.Icon)
	suite.Equal(canCreate, createdGrant.CanCreate)
	suite.Equal(allowDuplicateNames, createdGrant.AllowDuplicateNames)

	suite.createdGrantCategory = createdGrant
}

func (suite *GrantCategoryServiceTestSuite) TestB_GetGrantCategory() {
    ctx := suite.T().Context()
    createdGrantCategory := suite.createdGrantCategory
	// Fail the test if no grant category was created in previous test
	if createdGrantCategory == nil {
		suite.FailNow("No GrantCategory created to test GetGrantCategory")
	}

    retrievedGrantCategory, err := suite.GrantCategoryClient.GetGrantCategory(ctx, createdGrantCategory.Id)

    suite.NoError(err, "Failed to get GrantCategory with id %s: %v", createdGrantCategory.Id, err)
	
    suite.Equal(createdGrantCategory.Id, retrievedGrantCategory.Id)
    suite.Equal(createdGrantCategory.Name, retrievedGrantCategory.Name)
    suite.Equal(createdGrantCategory.Description, retrievedGrantCategory.Description)
    suite.Equal(createdGrantCategory.Icon, retrievedGrantCategory.Icon)
    suite.Equal(createdGrantCategory.CanCreate, retrievedGrantCategory.CanCreate)
    suite.Equal(createdGrantCategory.AllowDuplicateNames, retrievedGrantCategory.AllowDuplicateNames)
}

func (suite *GrantCategoryServiceTestSuite) TestC_UpdateGrantCategory() {
    ctx := suite.T().Context()
    createdGrantCategoryId := suite.createdGrantCategory.Id

    updatedName := suite.createdGrantCategory.Name + " Updated"
    updatedDescription := suite.createdGrantCategory.Description + " Updated"
    updateInput := &schema.GrantCategoryInput{
        Name:        &updatedName,
        Description: &updatedDescription,
    }

    updatedGrantCategory, err := suite.GrantCategoryClient.UpdateGrantCategory(ctx, createdGrantCategoryId, *updateInput)
    suite.NoErrorf(err, "Failed to update Test Grant with id %s: %v", createdGrantCategoryId, err)

    suite.Require().NotNil(updatedGrantCategory)
    suite.Require().Equal(createdGrantCategoryId, updatedGrantCategory.Id)
    suite.Require().Equal(updatedName, updatedGrantCategory.Name)
    suite.Require().Equal(updatedDescription, updatedGrantCategory.Description)

    // Update the suite's createdGrantCategory for subsequent tests
    suite.createdGrantCategory = updatedGrantCategory
}

func (suite *GrantCategoryServiceTestSuite) TestD_ListGrantCategories() {
    ctx := suite.T().Context()

    grantCategories, err := suite.GrantCategoryClient.ListGrantCategories(ctx)
    if err != nil {
        suite.FailNow("Failed to list GrantCategories: %v", err)
    }

    suite.NotEmpty(grantCategories)
	expectedName := suite.createdGrantCategory.Name
	var retrievedNames []string
    for _, category := range grantCategories {
        retrievedNames = append(retrievedNames, category.Name)
    }
	suite.Containsf(retrievedNames, expectedName, "Created grant category not found in list by name %s", expectedName)
}

func (suite *GrantCategoryServiceTestSuite) TestE_DeleteGrantCategory() {
    ctx := suite.T().Context()
    createdGrantCategory := suite.createdGrantCategory

    err := suite.GrantCategoryClient.DeleteGrantCategory(ctx, createdGrantCategory.Id)
    suite.NoError(err)

    // Verify that the grant category is actually deleted
    _, err = suite.GrantCategoryClient.GetGrantCategory(ctx, createdGrantCategory.Id)
    suite.Require().Error(err, "Expected an error when getting a deleted grant category")
}

func TestGrantCategoryServiceTestSuite(t *testing.T) {
	suite.Run(t, new(GrantCategoryServiceTestSuite))
}
