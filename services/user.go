package services

import (
	"context"
	"fmt"

	"github.com/Khan/genqlient/graphql"

	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/types"
	"github.com/collibra/data-access-go-sdk/utils"
)

type UserClient struct {
	client graphql.Client
}

func NewUserClient(client graphql.Client) *UserClient {
	return &UserClient{
		client: client,
	}
}

func (c *UserClient) GetCurrentUser(ctx context.Context) (*types.User, error) {
	result, err := schema.CurrentUser(ctx, c.client)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	return &result.CurrentUser.User, nil
}

// GetUser returns the user with the given ID.
// Returns a User if the user is found, otherwise returns an error.
func (c *UserClient) GetUser(ctx context.Context, id string) (*types.User, error) {
	result, err := schema.GetUser(ctx, c.client, id)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch r := result.User.(type) {
	case *schema.GetUserUser:
		return &r.User, nil
	case *schema.GetUserUserNotFoundError:
		return nil, types.NewErrNotFound(id, r.Typename, r.Message)
	case *schema.GetUserUserPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("getUser", r.Message)
	case *schema.GetUserUserInvalidEmailError:
		return nil, types.NewErrInvalidEmail(r.ErrEmail, r.Message)
	case *schema.GetUserUserInvalidInputError:
		return nil, types.NewErrInvalidInput(r.Message)
	default:
		return nil, types.NewErrClient(fmt.Errorf("unexpected result type: %T", r))
	}
}

// GetUserByEmail Get a user by their email address.
// Returns a User if user is found, otherwise returns an error.
func (c *UserClient) GetUserByEmail(ctx context.Context, email string) (*types.User, error) {
	result, err := schema.GetUserByEmail(ctx, c.client, email)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	if result.UserByEmail == nil {
		return nil, types.NewErrNotFound(email, utils.Ptr("user"), "No user found for the given email address.")
	}

	switch user := (*result.UserByEmail).(type) {
	case *schema.GetUserByEmailUserByEmailUser:
		return &user.User, nil
	case *schema.GetUserByEmailUserByEmailInvalidEmailError:
		return nil, types.NewErrInvalidEmail(user.ErrEmail, user.Message)
	case *schema.GetUserByEmailUserByEmailNotFoundError:
		return nil, types.NewErrNotFound(email, user.Typename, user.Message)
	case *schema.GetUserByEmailUserByEmailPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("getUserByEmail", user.Message)
	default:
		return nil, types.NewErrClient(fmt.Errorf("unexpected result type: %T", user))
	}
}

// CreateUser creates a new user in Collibra Data Access
// Returns a User if user is created successfully, otherwise returns an error.
func (c *UserClient) CreateUser(ctx context.Context, userInput types.UserInput) (*types.User, error) {
	result, err := schema.CreateUser(ctx, c.client, userInput)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch user := result.CreateUser.(type) {
	case *schema.CreateUserCreateUser:
		return &user.User, nil
	case *schema.CreateUserCreateUserInvalidEmailError:
		return nil, types.NewErrInvalidEmail(user.ErrEmail, user.Message)
	case *schema.CreateUserCreateUserPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("createUser", user.Message)
	case *schema.CreateUserCreateUserNotFoundError:
		return nil, types.NewErrNotFound("", user.Typename, user.Message)
	default:
		return nil, types.NewErrClient(fmt.Errorf("unexpected result type: %T", user))
	}
}

// UpdateUser updates an existing user in Collibra Data Access
// Returns a User if user is updated successfully, otherwise returns an error.
func (c *UserClient) UpdateUser(ctx context.Context, id string, userInput types.UserInput) (*types.User, error) {
	result, err := schema.UpdateUser(ctx, c.client, id, userInput)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch user := result.UpdateUser.(type) {
	case *schema.UpdateUserUpdateUser:
		return &user.User, nil
	case *schema.UpdateUserUpdateUserInvalidEmailError:
		return nil, types.NewErrInvalidEmail(user.ErrEmail, user.Message)
	case *schema.UpdateUserUpdateUserNotFoundError:
		return nil, types.NewErrNotFound(id, user.Typename, user.Message)
	case *schema.UpdateUserUpdateUserPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("updateUser", user.Message)
	default:
		return nil, types.NewErrClient(fmt.Errorf("unexpected result type: %T", user))
	}
}
