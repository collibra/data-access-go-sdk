package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	idp "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
)

type userTokens struct {
	userName     string
	idToken      string
	refreshToken string
	expiration   *time.Time
}

type orgInfo struct {
	AuthOrgId   string
	ClientAppId string
}

type AuthedDoer struct {
	Domain string
	User   string
	Secret string
	Url    string

	clientAppId string

	token *userTokens
}

func (d *AuthedDoer) Do(req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", "Raito SDK")
	req.Header.Set("Raito-Domain", d.Domain)

	err := d.addTokenToHeader(req.Context(), &req.Header)
	if err != nil {
		return nil, fmt.Errorf("get token: %w", err)
	}

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error while doing HTTP POST to %q: %s", req.URL.String(), err.Error())
	}

	return resp, nil
}

func (d *AuthedDoer) addTokenToHeader(ctx context.Context, h *http.Header) error {
	if d.token == nil {
		d.token = &userTokens{userName: d.User}
	}

	err := d.updateToken(ctx)
	if err != nil {
		return fmt.Errorf("update token: %w", err)
	}

	h.Add("Authorization", "token "+d.token.idToken)

	return nil
}

func (d *AuthedDoer) updateToken(ctx context.Context) error {
	if checkTokenValidity(d.token) {
		return nil
	}

	if d.clientAppId == "" {
		clientAppId, err := fetchClientAppId(d.Url, d.Domain)
		if err != nil {
			return fmt.Errorf("fetch client app id: %w", err)
		}

		d.clientAppId = clientAppId
	}

	if d.token.refreshToken != "" {
		err := d.refreshToken(ctx)
		if err != nil {
			return fmt.Errorf("refresh token: %w", err)
		}
	} else {
		err := d.fetchNewToken(ctx)
		if err != nil {
			return fmt.Errorf("fetch new token: %w", err)
		}
	}

	return nil
}

func (d *AuthedDoer) fetchNewToken(ctx context.Context) error {
	cfg, err := loadConfig(ctx)
	if err != nil {
		return err
	}

	idpClient := idp.NewFromConfig(cfg)
	output, err := idpClient.InitiateAuth(context.TODO(), &idp.InitiateAuthInput{
		AuthFlow:       "USER_PASSWORD_AUTH",
		ClientId:       &d.clientAppId,
		AuthParameters: map[string]string{"USERNAME": d.User, "PASSWORD": d.Secret},
	})

	if err != nil {
		return fmt.Errorf("error while initiating authentication flow for user %q: %w", d.User, err)
	}

	err = handleAuthOutput(output, d.token)
	if err != nil {
		return fmt.Errorf("error while handling authentication output: %w", err)
	}

	return nil
}

func (d *AuthedDoer) refreshToken(ctx context.Context) error {
	cfg, err := loadConfig(ctx)
	if err != nil {
		return err
	}

	idpClient := idp.NewFromConfig(cfg)
	output, err := idpClient.InitiateAuth(ctx, &idp.InitiateAuthInput{
		AuthFlow:       "REFRESH_TOKEN_AUTH",
		ClientId:       &d.clientAppId,
		AuthParameters: map[string]string{"REFRESH_TOKEN": d.token.refreshToken},
	})

	if err != nil {
		return fmt.Errorf("error while initiating authentication flow for user %q: %w", d.token.userName, err)
	}

	err = handleAuthOutput(output, d.token)
	if err != nil {
		return fmt.Errorf("error while handling authentication output: %w", err)
	}

	return nil
}

func loadConfig(ctx context.Context) (aws.Config, error) {
	// TODO configurable region
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("eu-central-1"))
	if err != nil {
		return aws.Config{}, fmt.Errorf("error while configuring AWS SDK: %w", err)
	}

	return cfg, nil
}

func handleAuthOutput(output *idp.InitiateAuthOutput, tokens *userTokens) error {
	if output.AuthenticationResult != nil {
		if output.AuthenticationResult.IdToken == nil {
			return fmt.Errorf("no id token found in authentication result")
		}

		if output.AuthenticationResult.RefreshToken != nil {
			tokens.refreshToken = *output.AuthenticationResult.RefreshToken
		}

		tokens.idToken = *output.AuthenticationResult.IdToken
		e := time.Now().Add(time.Second * time.Duration(output.AuthenticationResult.ExpiresIn))
		tokens.expiration = &e

		return nil
	} else {
		return fmt.Errorf("invalid authentication result received (challenge %q)", output.ChallengeName)
	}
}

func fetchClientAppId(urlBase, domain string) (string, error) {
	if domain == "" {
		return "", fmt.Errorf("no domain specified")
	}

	domain = strings.ToLower(domain)
	if !isValidDomain(domain) {
		return "", fmt.Errorf("invalid domain name %q. A domain should start with a letter and can only contain alphanumeric characters and the dash character. It also should not end with a dash character", domain)
	}

	if !strings.HasSuffix(urlBase, "/") {
		urlBase += "/"
	}

	url := urlBase + "admin/org/" + domain

	req, err := http.NewRequest("GET", url, http.NoBody)
	if err != nil {
		return "", fmt.Errorf("error while creating HTTP GET request to %q: %s", url, err.Error())
	}

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error while doing HTTP GET to %q: %s", url, err.Error())
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("unexpected status code %d received when calling URL %q", resp.StatusCode, url)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("error while reading body for call to %q: %s", url, err.Error())
	}

	org := orgInfo{}

	err = json.Unmarshal(body, &org)
	if err != nil {
		return "", fmt.Errorf("error while parsing organization info response from %q: %s", url, err.Error())
	}

	return org.ClientAppId, nil
}

func isValidDomain(domain string) bool {
	matched, err := regexp.Match("^[a-z][a-z0-9-]*[a-z0-9]$", []byte(domain))
	if err != nil {
		return false
	}

	return matched
}

func checkTokenValidity(token *userTokens) bool {
	if token.idToken == "" || token.refreshToken == "" || token.expiration == nil {
		return false
	}

	now := time.Now().Add(time.Second * 10)

	return now.Before(*token.expiration)
}
