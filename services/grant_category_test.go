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

	GrantCategoryClient *services.GrantCategoryClient
}

func TestGrantCategoryServiceTestSuite(t *testing.T) {
	suite.Run(t, new(GrantCategoryServiceTestSuite))
}

func (suite *GrantCategoryServiceTestSuite) TestGrantCategory() {
	url, clientOptions := utils.GetEnvConfig(&suite.Suite)
	client := sdk.NewClient(url, clientOptions...)

	if client == nil {
		suite.FailNow("Failed to create Collibra client")
	}

	grantCategoryClient := client.GrantCategory()
	if grantCategoryClient == nil {
		suite.FailNow("Failed to create GrantCategory client")
	}

	ctx := suite.T().Context()

	var createdGrantCategory *schema.GrantCategoryDetails

	suite.Run("Create Grant Category", func() {
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

		createdGrant, err := grantCategoryClient.CreateGrantCategory(ctx, *input)

		suite.Require().NoError(err, "Failed to create GrantCategory")

		suite.NotNil(createdGrant)
		suite.Equal(name, createdGrant.Name)
		suite.Equal(description, createdGrant.Description)
		suite.Equal(icon, createdGrant.Icon)
		suite.Equal(canCreate, createdGrant.CanCreate)
		suite.Equal(allowDuplicateNames, createdGrant.AllowDuplicateNames)

		createdGrantCategory = createdGrant
	})

	suite.Run("Get Grant Category", func() {
		// Skip the test if no grant category was created in previous test
		if createdGrantCategory == nil {
			suite.T().Skip("No GrantCategory created to test GetGrantCategory")
		}

		retrievedGrantCategory, err := grantCategoryClient.GetGrantCategory(ctx, createdGrantCategory.Id)

		suite.Require().NoError(err, "Failed to get GrantCategory with id %s: %v", createdGrantCategory.Id, err)
		suite.Require().NotNil(retrievedGrantCategory, "Retrieved GrantCategory is nil")
		suite.Equal(createdGrantCategory.Id, retrievedGrantCategory.Id)
		suite.Equal(createdGrantCategory.Name, retrievedGrantCategory.Name)
		suite.Equal(createdGrantCategory.Description, retrievedGrantCategory.Description)
		suite.Equal(createdGrantCategory.Icon, retrievedGrantCategory.Icon)
		suite.Equal(createdGrantCategory.CanCreate, retrievedGrantCategory.CanCreate)
		suite.Equal(createdGrantCategory.AllowDuplicateNames, retrievedGrantCategory.AllowDuplicateNames)
	})

	suite.Run("Update Grant Category", func() {
		updatedName := createdGrantCategory.Name + " Updated"
		updatedDescription := createdGrantCategory.Description + " Updated"
		updateInput := &schema.GrantCategoryInput{
			Name:        &updatedName,
			Description: &updatedDescription,
		}

		updatedGrantCategory, err := grantCategoryClient.UpdateGrantCategory(ctx, createdGrantCategory.Id, *updateInput)
		suite.Require().NoErrorf(err, "Failed to update Test Grant with id %s: %v", createdGrantCategory.Id, err)

		suite.Require().NotNil(updatedGrantCategory)
		suite.Require().Equal(createdGrantCategory.Id, updatedGrantCategory.Id)
		suite.Require().Equal(updatedName, updatedGrantCategory.Name)
		suite.Require().Equal(updatedDescription, updatedGrantCategory.Description)

		// Update the suite's createdGrantCategory for subsequent tests
		createdGrantCategory = updatedGrantCategory
	})

	suite.Run("List Grant Categories", func() {
		grantCategories, err := grantCategoryClient.ListGrantCategories(ctx)
		if err != nil {
			suite.Failf("Failed to list GrantCategories", "Error: %v", err)
		}

		suite.NotEmpty(grantCategories)

		expectedName := createdGrantCategory.Name

		retrievedNames := make([]string, 0, len(grantCategories))
		for i := range grantCategories {
			retrievedNames = append(retrievedNames, grantCategories[i].Name)
		}

		suite.Containsf(retrievedNames, expectedName, "Created grant category not found in list by name %s", expectedName)
	})

	suite.Run("Delete Grant Category", func() {
		err := grantCategoryClient.DeleteGrantCategory(ctx, createdGrantCategory.Id)
		suite.Require().NoError(err)

		// Verify that the grant category is actually deleted
		_, err = grantCategoryClient.GetGrantCategory(ctx, createdGrantCategory.Id)
		suite.Require().Error(err, "Expected an error when getting a deleted grant category")
	})
}
