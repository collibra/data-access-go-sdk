package e2e_test

import (
	"fmt"
	"testing"

	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/test/e2e/utils"
	"github.com/google/uuid"
)

func printUser(prefix string, user *schema.User) {
	emailValue := ""
	if user.Email != nil {
		emailValue = *user.Email
	}
	fmt.Printf("%s: ID=%s,\nName=%s,\nEmail=%s,\nType=%s\n",
		prefix, user.Id, user.Name, emailValue, user.Type)
}

func TestUserService(t *testing.T) {

	usersClient := client.User()

	var createdUser schema.User // placeholder for user to be used in tests
	t.Run("GetCurrentUser", func(t *testing.T) {
		// Test assumes we are authenticated as Admin user

		user, err := usersClient.GetCurrentUser(ctx)
		if err != nil {
			t.Fatalf("Failed to get current user: %v", err)
		}

		printUser("Current User", user)

		if user == nil {
			t.Fatal("Expected non-nil user")
		}

		if user.Id == "" {
			t.Errorf("Expected non-empty Id, got %s", user.Id)
		}
		expectedName := "Admin Istrator"
		if user.Name != expectedName {
			t.Errorf("Expected Name to be %s, got %s", expectedName, user.Name)
		}

		if user.Email == nil {
			t.Errorf("Expected non-nil Email, got nil")
		}

		expectedType := schema.UserTypeHuman
		if user.Type != expectedType {
			t.Errorf("Expected Type to be %s, got %s", expectedType, user.Type)
		}
	})

	t.Run("CreateUser", func(t *testing.T) {
		uuidString := uuid.NewString()
		userName := "SDK Automated Test User " + uuidString
		userEmail := "sdk_automated_test_user_" + uuidString + "@collibra.com"
		userType := schema.UserTypeHuman

		user, err := usersClient.CreateUser(ctx, schema.UserInput{
			Name:  &userName,
			Email: &userEmail,
			Type:  &userType,
		})

		if err != nil || user == nil {
			t.Fatalf("Failed to create user: %v", err)
		}

		printUser("User Created", user)

		createdUser = *user
	})

	t.Run("GetUser", func(t *testing.T) {
		if createdUser.Id == "" {
			t.Skip("Created user ID is empty, cannot proceed with GetUser test")
		}
		userData, err := usersClient.GetUser(ctx, createdUser.Id)
		if err != nil {
			t.Fatalf("Failed to get user: %v", err)
		}

		printUser("User Data", userData)

		if userData.Id != createdUser.Id {
			t.Errorf("Expected user ID to be %s, got %s", createdUser.Id, userData.Id)
		}
		if userData.Name != createdUser.Name {
			t.Errorf("Expected user name to be %s, got %s", createdUser.Name, userData.Name)
		}
		if !utils.CompareStringPointers(userData.Email, createdUser.Email) {
			t.Errorf("Expected user email to be %s, got %s", *createdUser.Email, *userData.Email)
		}
		if userData.Type != createdUser.Type {
			t.Errorf("Expected user type to be %s, got %s", createdUser.Type, userData.Type)
		}
	})

	t.Run("GetUserByEmail", func(t *testing.T) {
		if createdUser.Email == nil {
			t.Skip("Created user email is nil, cannot proceed with GetUserByEmail test")
		}
		userData, err := usersClient.GetUserByEmail(ctx, *createdUser.Email)
		if err != nil {
			t.Fatalf("Failed to get user by email: %v", err)
		}

		fmt.Printf("User Data by Email: %+v\n", userData)

		if userData.Id != createdUser.Id {
			t.Errorf("Expected user ID to be %s, got %s", createdUser.Id, userData.Id)
		}
		if userData.Name != createdUser.Name {
			t.Errorf("Expected user name to be %s, got %s", createdUser.Name, userData.Name)
		}
		if !utils.CompareStringPointers(userData.Email, createdUser.Email) {
			t.Errorf("Expected user email to be %s, got %s", *createdUser.Email, *userData.Email)
		}
		if userData.Type != createdUser.Type {
			t.Errorf("Expected user type to be %s, got %s", createdUser.Type, userData.Type)
		}
	})

	t.Run("UpdateUser", func(t *testing.T) {
		if createdUser.Id == "" {
			t.Skip("Created user ID is empty, cannot proceed with UpdateUser test")
		}
		newName := "Updated User Name"
		updatedUser, err := usersClient.UpdateUser(ctx, createdUser.Id, schema.UserInput{
			Name: &newName,
		})

		if err != nil {
			t.Fatalf("Failed to update user: %v", err)
		}

		printUser("Updated User", updatedUser)

		userData, err := usersClient.GetUser(ctx, createdUser.Id)

		if err != nil {
			t.Fatalf("Failed to get user after update: %v", err)
		}

		printUser("User Data After Update", userData)

		if userData.Name != newName {
			t.Errorf("Expected user name to be updated to %s, got %s", newName, userData.Name)
		}
	})
}
