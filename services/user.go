package services

import (
	"context"
	"errors"
	"fmt"

	"github.com/Khan/genqlient/graphql"
	"github.com/aws/smithy-go/ptr"

	"github.com/raito-io/sdk-go/internal/schema"
	"github.com/raito-io/sdk-go/types"
)

type UserClient struct {
	client graphql.Client
}

func NewUserClient(client graphql.Client) UserClient {
	return UserClient{
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
		return nil, types.NewErrNotFound(email, ptr.String("user"), "No user found for the given email address.")
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

// CreateUser creates a new user in Raito Cloud
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

// UpdateUser updates an existing user in Raito Cloud
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

// DeleteUser deletes an existing user from Raito Cloud
// Returns nil if user is deleted successfully, otherwise returns an error.
func (c *UserClient) DeleteUser(ctx context.Context, id string) error {
	result, err := schema.DeleteUser(ctx, c.client, id)
	if err != nil {
		return types.NewErrClient(err)
	}

	switch response := result.DeleteUser.(type) {
	case *schema.DeleteUserDeleteUserUserDelete:
		if response.Success {
			return nil
		} else {
			return types.NewErrClient(errors.New("unknown user delete error"))
		}
	case *schema.DeleteUserDeleteUserPermissionDeniedError:
		return types.NewErrPermissionDenied("deleteUser", response.Message)
	default:
		return types.NewErrClient(fmt.Errorf("unexpected response type: %T", response))
	}
}

type InviteAsRaitoUserOptions struct {
	NoPassword bool
}

// WithInviteAsRaitoUserNoPassword adds the option to not send a password to the invited user.
func WithInviteAsRaitoUserNoPassword() func(options *InviteAsRaitoUserOptions) {
	return func(options *InviteAsRaitoUserOptions) {
		options.NoPassword = true
	}
}

// InviteAsRaitoUser invites an existing user as Raito User
// id is the id of the user to invite.
// WithInviteAsRaitoUserNoPassword can be used to add the option to not send a password to the invited user.
func (c *UserClient) InviteAsRaitoUser(ctx context.Context, id string, ops ...func(*InviteAsRaitoUserOptions)) (*types.User, error) {
	options := InviteAsRaitoUserOptions{}
	for _, op := range ops {
		op(&options)
	}

	result, err := schema.InviteAsRaitoUser(ctx, c.client, id, &options.NoPassword)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch user := result.InviteAsRaitoUser.(type) {
	case *schema.InviteAsRaitoUserInviteAsRaitoUser:
		return &user.User, nil
	case *schema.InviteAsRaitoUserInviteAsRaitoUserPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("InviteRaitoUser", user.Message)
	case *schema.InviteAsRaitoUserInviteAsRaitoUserNotFoundError:
		return nil, types.NewErrNotFound(id, user.Typename, user.Message)
	case *schema.InviteAsRaitoUserInviteAsRaitoUserInvalidEmailError:
		return nil, types.NewErrInvalidEmail(user.ErrEmail, user.Message)
	default:
		return nil, types.NewErrClient(fmt.Errorf("unexpected response type: %T", user))
	}
}

// RemoveAsRaitoUser removes an existing user from Raito Cloud
// id is the id of the user to remove.
func (c *UserClient) RemoveAsRaitoUser(ctx context.Context, id string) (*types.User, error) {
	result, err := schema.RemoveAsRaitoUser(ctx, c.client, id)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch user := result.RemoveAsRaitoUser.(type) {
	case *schema.RemoveAsRaitoUserRemoveAsRaitoUser:
		return &user.User, nil
	case *schema.RemoveAsRaitoUserRemoveAsRaitoUserPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("removeAsRaitoUser", user.Message)
	case *schema.RemoveAsRaitoUserRemoveAsRaitoUserInvalidEmailError:
		return nil, types.NewErrInvalidEmail(user.ErrEmail, user.Message)
	case *schema.RemoveAsRaitoUserRemoveAsRaitoUserNotFoundError:
		return nil, types.NewErrNotFound(id, user.Typename, user.Message)
	default:
		return nil, types.NewErrClient(fmt.Errorf("unexpected response type: %T", user))
	}
}

// SetUserPassword sets the password for an existing user in Raito Cloud
// id is the id of the user to set the password for.
// password is the password to set for the user.
func (c *UserClient) SetUserPassword(ctx context.Context, id string, password string) (*types.User, error) {
	result, err := schema.SetUserPassword(ctx, c.client, id, password)
	if err != nil {
		return nil, types.NewErrClient(err)
	}

	switch user := result.SetPassword.(type) {
	case *schema.SetUserPasswordSetPasswordUser:
		return &user.User, nil
	case *schema.SetUserPasswordSetPasswordPermissionDeniedError:
		return nil, types.NewErrPermissionDenied("setUserPassword", user.Message)
	case *schema.SetUserPasswordSetPasswordNotFoundError:
		return nil, types.NewErrNotFound(id, user.Typename, user.Message)
	case *schema.SetUserPasswordSetPasswordInvalidEmailError:
		return nil, types.NewErrInvalidEmail(user.ErrEmail, user.Message)
	default:
		return nil, types.NewErrClient(fmt.Errorf("unexpected response type: %T", user))
	}
}
