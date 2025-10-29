package services_test

import (
	"fmt"
	"log"
	"os"
	"testing"

	sdk "github.com/collibra/data-access-go-sdk"
	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/services"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type UserServiceTestSuite struct {
	suite.Suite
	UserClient *services.UserClient
}

func getEnv(key string) string {
	value := os.Getenv(key)

	if value == "" {
		log.Fatalf("Environment variable %s must be set for e2e tests", key)
	}

	return value
}

func (suite *UserServiceTestSuite) SetupSuite() {
	client := sdk.NewClient(
		getEnv("COLLIBRA_USER"),
		getEnv("COLLIBRA_PASSWORD"),
		getEnv("COLLIBRA_URL"),
	)

	if client == nil {
		log.Fatalf("Failed to create Collibra client")
	}
	userClient := client.User()
	if userClient == nil {
		log.Fatalf("Failed to create User client")
	}
	suite.UserClient = userClient
}

func printUser(prefix string, user *schema.User) {
	emailValue := ""
	if user.Email != nil {
		emailValue = *user.Email
	}
	fmt.Printf("%s: ID=%s,\nName=%s,\nEmail=%s,\nType=%s\n",
		prefix, user.Id, user.Name, emailValue, user.Type)
}

var createdUser schema.User // placeholder for user to be used in tests
func (suite *UserServiceTestSuite) TestGetCurrentUser() {
	// Test assumes we are authenticated as Admin user
	ctx := suite.T().Context()
	userClient := suite.UserClient
	assert := assert.New(suite.T())
	require := require.New(suite.T())

	user, err := userClient.GetCurrentUser(ctx)
	require.NoError(err, "Failed to get current user")

	printUser("Current User", user)

	require.NotNil(user)

	assert.NotEmpty(user.Id)
	expectedName := "Admin Istrator"
	assert.Equal(expectedName, user.Name)

	assert.NotNil(user.Email)

	expectedType := schema.UserTypeHuman
	assert.Equal(expectedType, user.Type)
}

func (suite *UserServiceTestSuite) TestCreateUser() {
	t := suite.T()
	ctx := t.Context()
	userClient := suite.UserClient
	require := require.New(t)

	uuidString := uuid.NewString()
	userName := "SDK Automated Test User " + uuidString
	userEmail := "sdk_automated_test_user_" + uuidString + "@collibra.com"
	userType := schema.UserTypeHuman

	user, err := userClient.CreateUser(ctx, schema.UserInput{
		Name:  &userName,
		Email: &userEmail,
		Type:  &userType,
	})

	require.NoError(err, "Failed to create user")
	require.NotNil(user)

	printUser("User Created", user)

	createdUser = *user
}

func (suite *UserServiceTestSuite) TestGetUser() {
	if createdUser.Id == "" {
		suite.T().Skip("Created user ID is empty, cannot proceed with GetUser test")
	}
	t := suite.T()
	ctx := t.Context()
	assert := assert.New(t)
	require := require.New(t)
	userClient := suite.UserClient

	userData, err := userClient.GetUser(ctx, createdUser.Id)
	require.NoError(err, "Failed to get user")

	printUser("User Data", userData)

	assert.Equal(createdUser.Id, userData.Id)
	assert.Equal(createdUser.Name, userData.Name)
	assert.Equal(createdUser.Email, userData.Email)
	assert.Equal(createdUser.Type, userData.Type)
}

func (suite *UserServiceTestSuite) TestGetUserByEmail() {
	if createdUser.Email == nil {
		suite.T().Skip("Created user email is nil, cannot proceed with GetUserByEmail test")
	}
	t := suite.T()
	ctx := t.Context()
	assert := assert.New(t)
	require := require.New(t)
	userClient := suite.UserClient

	userData, err := userClient.GetUserByEmail(ctx, *createdUser.Email)
	require.NoError(err, "Failed to get user by email")

	fmt.Printf("User Data by Email: %+v\n", userData)

	assert.Equal(createdUser.Id, userData.Id)
	assert.Equal(createdUser.Name, userData.Name)
	assert.Equal(createdUser.Email, userData.Email)
	assert.Equal(createdUser.Type, userData.Type)
}

func (suite *UserServiceTestSuite) TestUpdateUser() {
	if createdUser.Id == "" {
		suite.T().Skip("Created user ID is empty, cannot proceed with UpdateUser test")
	}
	t := suite.T()
	ctx := t.Context()
	assert := assert.New(t)
	require := require.New(t)
	userClient := suite.UserClient

	newName := "Updated User Name"
	updatedUser, err := userClient.UpdateUser(ctx, createdUser.Id, schema.UserInput{
		Name: &newName,
	})

	require.NoError(err, "Failed to update user")

	printUser("Updated User", updatedUser)

	userData, err := userClient.GetUser(ctx, createdUser.Id)

	require.NoError(err, "Failed to get user after update")

	printUser("User Data After Update", userData)

	assert.Equal(newName, userData.Name)
}

func TestUserServiceTestSuite(t *testing.T) {
	suite.Run(t, new(UserServiceTestSuite))
}
