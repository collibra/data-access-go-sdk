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

type UserServiceTestSuite struct {
	suite.Suite

	UserClient  *services.UserClient
	createdUser schema.User
}

func (suite *UserServiceTestSuite) SetupSuite() {
	config := utils.GetEnvConfig(&suite.Suite)
	client := sdk.NewClient(
		config.User,
		config.Password,
		config.URL,
	)

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

	expectedName := "Admin Istrator"
	suite.Equal(expectedName, user.Name)

	suite.NotNil(user.Email)

	expectedType := schema.UserTypeHuman
	suite.Equal(expectedType, user.Type)
}

func (suite *UserServiceTestSuite) TestA_CreateUser() {
	t := suite.T()
	ctx := t.Context()
	userClient := suite.UserClient

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

	suite.createdUser = *user
}

func (suite *UserServiceTestSuite) TestB_GetUser() {
	createdUser := suite.createdUser
	if createdUser.Id == "" {
		suite.T().Skip("Created user ID is empty, cannot proceed with GetUser test")
	}

	t := suite.T()
	ctx := t.Context()

	userClient := suite.UserClient

	userData, err := userClient.GetUser(ctx, createdUser.Id)
	suite.Require().NoError(err, "Failed to get user")

	printUser(t, "User Data", userData)

	suite.Equal(createdUser.Id, userData.Id)
	suite.Equal(createdUser.Name, userData.Name)
	suite.Equal(createdUser.Email, userData.Email)
	suite.Equal(createdUser.Type, userData.Type)
}

func (suite *UserServiceTestSuite) TestC_GetUserByEmail() {
	createdUser := suite.createdUser
	if createdUser.Email == nil {
		suite.T().Skip("Created user email is nil, cannot proceed with GetUserByEmail test")
	}

	t := suite.T()
	ctx := t.Context()

	userClient := suite.UserClient

	userData, err := userClient.GetUserByEmail(ctx, *createdUser.Email)
	suite.Require().NoError(err, "Failed to get user by email")

	printUser(t, "User Data by Email", userData)

	suite.Equal(createdUser.Id, userData.Id)
	suite.Equal(createdUser.Name, userData.Name)
	suite.Equal(createdUser.Email, userData.Email)
	suite.Equal(createdUser.Type, userData.Type)
}

func (suite *UserServiceTestSuite) TestD_UpdateUser() {
	createdUser := suite.createdUser
	if createdUser.Id == "" {
		suite.T().Skip("Created user ID is empty, cannot proceed with UpdateUser test")
	}

	t := suite.T()
	ctx := t.Context()

	userClient := suite.UserClient

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
}

func TestUserServiceTestSuite(t *testing.T) {
	suite.Run(t, new(UserServiceTestSuite))
}
