package services_test

import (
	"testing"


"github.com/google/uuid"
"github.com/stretchr/testify/suite"

sdk "github.com/collibra/data-access-go-sdk"
"github.com/collibra/data-access-go-sdk/internal/schema"
"github.com/collibra/data-access-go-sdk/services"
"github.com/collibra/data-access-go-sdk/utils"
)

type UserServiceTestSuite struct {
	suite.Suite

	UserClient *services.UserClient
}

func (suite *UserServiceTestSuite) SetupSuite() {
	url, clientOptions := utils.GetEnvConfig(&suite.Suite)
	client := sdk.NewClient(url, clientOptions...)

	if client == nil {
		suite.FailNow("Failed to create Collibra client")
	}

	userClient := client.User()
	if userClient == nil {
		suite.FailNow("Failed to create User client")
	}

	suite.UserClient = userClient
}

func printUser(t *testing.T, prefix string, user *schema.User) {
	t.Helper()

	emailValue := ""
	if user.Email != nil {
		emailValue = *user.Email
	}

	t.Logf("%s: ID=%s, Name=%s, Email=%s, Type=%s\n",
		prefix, user.Id, user.Name, emailValue, user.Type)
}

func TestUserServiceTestSuite(t *testing.T) {
	suite.Run(t, new(UserServiceTestSuite))
}

func (suite *UserServiceTestSuite) TestGetCurrentUser() {
	// Test assumes we are authenticated as Admin user
	t := suite.T()
	ctx := t.Context()
	userClient := suite.UserClient

	user, err := userClient.GetCurrentUser(ctx)
	suite.Require().NoError(err, "Failed to get current user")

	printUser(t, "Current User", user)

	suite.NotNil(user)

	suite.NotEmpty(user.Id)

	expectedName := "Data Access Go SDK"
	suite.Equal(expectedName, user.Name)

	suite.NotNil(user.Email)

	expectedType := schema.UserTypeHuman
	suite.Equal(expectedType, user.Type)
}

func (suite *UserServiceTestSuite) TestUsers() {
	t := suite.T()
	ctx := t.Context()
	userClient := suite.UserClient

	var createdUser schema.User

	suite.Run("Create a New User", func() {
		uuidString := uuid.NewString()
		userName := "SDK Automated Test User " + uuidString
		userEmail := "sdk_automated_test_user_" + uuidString + "@collibra.com"
		userType := schema.UserTypeHuman

		user, err := userClient.CreateUser(ctx, schema.UserInput{
			Name:  &userName,
			Email: &userEmail,
			Type:  &userType,
		})

		suite.Require().NoError(err, "Failed to create user")
		suite.NotNil(user)

		printUser(t, "User Created", user)

		createdUser = *user
	})

	suite.Run("Get User", func() {
		if createdUser.Id == "" {
			suite.T().Skip("Created user ID is empty, cannot proceed with GetUser test")
		}

		userData, err := userClient.GetUser(ctx, createdUser.Id)
		suite.Require().NoError(err, "Failed to get user")

		printUser(t, "User Data", userData)

		suite.Equal(createdUser.Id, userData.Id)
		suite.Equal(createdUser.Name, userData.Name)
		suite.Equal(createdUser.Email, userData.Email)
		suite.Equal(createdUser.Type, userData.Type)
	})

	suite.Run("Get User By Email", func() {
		if createdUser.Email == nil {
			suite.T().Skip("Created user email is nil, cannot proceed with GetUserByEmail test")
		}

		userData, err := userClient.GetUserByEmail(ctx, *createdUser.Email)
		suite.Require().NoError(err, "Failed to get user by email")

		printUser(t, "User Data by Email", userData)

		suite.Equal(createdUser.Id, userData.Id)
		suite.Equal(createdUser.Name, userData.Name)
		suite.Equal(createdUser.Email, userData.Email)
		suite.Equal(createdUser.Type, userData.Type)
	})

	suite.Run("Update User", func() {
		if createdUser.Id == "" {
			suite.T().Skip("Created user ID is empty, cannot proceed with UpdateUser test")
		}

		newName := "Updated User Name"
		updatedUser, err := userClient.UpdateUser(ctx, createdUser.Id, schema.UserInput{
			Name: &newName,
		})

		suite.Require().NoError(err, "Failed to update user")

		printUser(t, "Updated User", updatedUser)

		userData, err := userClient.GetUser(ctx, createdUser.Id)

		suite.Require().NoError(err, "Failed to get user after update")

		printUser(t, "User Data After Update", userData)

		suite.Require().Equal(newName, userData.Name)
	})

	suite.Run("Error should be reported for duplicate email on user creation", func() {
		t := suite.T()
		ctx := t.Context()
		userClient := suite.UserClient

		if createdUser.Id == "" {
			suite.T().Skip("Created user ID is empty, cannot proceed with test")
		}

		uuidString := uuid.NewString()
		userName := "SDK Automated Test User " + uuidString
		userEmail := *createdUser.Email
		userType := schema.UserTypeHuman

		user, err := userClient.CreateUser(ctx, schema.UserInput{
			Name:  &userName,
			Email: &userEmail,
			Type:  &userType,
		})
		suite.Require().Nil(user)
		suite.Require().Error(err)
		// TODO: Add a message check once a general one is improved
	})
}

func (suite *UserServiceTestSuite) TestGetNonExistentUser() {
	t := suite.T()
	ctx := t.Context()
	userClient := suite.UserClient
	userData, err := userClient.GetUser(ctx, "nonexistent_user")
	suite.Require().Nil(userData)
	suite.Require().Error(err)
	suite.Require().ErrorContains(err, "Requested user not found")
}

func (suite *UserServiceTestSuite) TestGetNonExistentUserByEmail() {
	t := suite.T()
	ctx := t.Context()
	userClient := suite.UserClient
	user, err := userClient.GetUserByEmail(ctx, "Idonotexists@ghjghjg.com")
	suite.Require().Nil(user)
	suite.Require().Error(err)
	suite.Require().ErrorContains(err, "Requested user not found")
}

func (suite *UserServiceTestSuite) TestUpdateNonExistentUser() {
	t := suite.T()
	ctx := t.Context()
	userClient := suite.UserClient
	newName := "Updated User Name"
	updatedUser, err := userClient.UpdateUser(ctx, "doesnot matter", schema.UserInput{
		Name: &newName,
	})
	suite.Require().Nil(updatedUser)
	suite.Require().Error(err)
	suite.Require().ErrorContains(err, "unexpected result type")
}
