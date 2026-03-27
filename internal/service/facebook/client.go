package facebook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.uber.org/zap"

	"deploy_data_bigquery/internal/logger"
)

const (
	baseURL = "https://graph.facebook.com/v25.0"
)

// Client is a low-level HTTP client for the Facebook Graph API.
type Client struct {
	accessToken string
	httpClient  *http.Client
	rateLimiter *RateLimiter
}

// NewClient creates a new Facebook Graph API client.
func NewClient(accessToken string) *Client {
	return &Client{
		accessToken: accessToken,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		rateLimiter: NewRateLimiter(),
	}
}

// HTTP response wrapper
type apiResponse struct {
	Data   json.RawMessage `json:"data"`
	Paging *Paging         `json:"paging,omitempty"`
	Error  *APIError       `json:"error,omitempty"`
}

type Paging struct {
	Next string `json:"next"`
}

type APIError struct {
	Code           int    `json:"code"`
	Message        string `json:"message"`
	Type           string `json:"type"`
	FBTraceID      string `json:"fbtrace_id"`
	IsTransient    bool   `json:"is_transient"`
	ErrorSubcode   int    `json:"error_subcode"`
	ErrorUserTitle string `json:"error_user_title"`
	ErrorUserMsg   string `json:"error_user_msg"`
}

// getJSON performs a GET request to the Facebook Graph API, applies rate limiting,
// and retries on transient errors with exponential backoff.
func (c *Client) getJSON(ctx context.Context, path string, params url.Values) ([]byte, error) {
	return c.doRequest(ctx, http.MethodGet, path, params, nil)
}

// postJSON performs a POST request to the Facebook Graph API.
func (c *Client) postJSON(ctx context.Context, path string, params url.Values, body interface{}) ([]byte, error) {
	return c.doRequest(ctx, http.MethodPost, path, params, body)
}

// doRequest handles rate limiting, retries, and error handling for all API calls.
func (c *Client) doRequest(ctx context.Context, method, path string, params url.Values, body interface{}) ([]byte, error) {
	// Step 1: Rate limiter — blocks until a call slot is available
	if err := c.rateLimiter.Allow(ctx); err != nil {
		return nil, fmt.Errorf("rate limiter: %w", err)
	}

	// Step 2: Build URL
	params = withAuth(params, c.accessToken)
	u, err := url.Parse(baseURL + path)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}
	u.RawQuery = params.Encode()

	var bodyReader io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		bodyReader = bytes.NewReader(b)
	}

	// Step 3: HTTP request with retry
	var respBody []byte
	err = retry(3, time.Second, func() error {
		// Reset body reader to beginning before each retry attempt
		if body != nil {
			bodyReader.(io.Seeker).Seek(0, 0)
		}

		req, err := http.NewRequestWithContext(ctx, method, u.String(), bodyReader)
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		log := logger.GetLogger()
		log.Debug("FACEBOOK_API: requesting",
			zap.String("method", method),
			zap.String("path", path),
			zap.Int("rate_limiter_calls", c.rateLimiter.CallCount()),
		)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("http request: %w", err)
		}
		defer resp.Body.Close()

		respBody, err = io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("read body: %w", err)
		}

		// Check HTTP status
		if resp.StatusCode >= 500 {
			return fmt.Errorf("server error: %d", resp.StatusCode)
		}

		// Parse response for API-level errors
		var ar apiResponse
		if err := json.Unmarshal(respBody, &ar); err != nil {
			return fmt.Errorf("parse response: %w", err)
		}

		if ar.Error != nil {
			if ar.Error.IsTransient || ar.Error.Code == 4 || ar.Error.Code == 17 {
				// Rate limit or transient error — retry
				return fmt.Errorf("transient error [%d]: %s", ar.Error.Code, ar.Error.Message)
			}
			return fmt.Errorf("api error [%d] %s: %s", ar.Error.Code, ar.Error.Type, ar.Error.Message)
		}

		return nil // success
	})

	if err != nil {
		return nil, err
	}

	return respBody, nil
}

// paginate fetches all pages for a given endpoint, calling the provided handler
// for each page of raw JSON data.
func (c *Client) paginate(ctx context.Context, path string, params url.Values, handler func(data json.RawMessage) error) error {
	for {
		respBody, err := c.getJSON(ctx, path, params)
		if err != nil {
			return err
		}

		var ar apiResponse
		if err := json.Unmarshal(respBody, &ar); err != nil {
			return fmt.Errorf("parse paginated response: %w", err)
		}

		if err := handler(ar.Data); err != nil {
			return err
		}

		// Check for next page
		if ar.Paging == nil || ar.Paging.Next == "" {
			break
		}

		// Parse the next page URL
		nextURL, err := url.Parse(ar.Paging.Next)
		if err != nil {
			break
		}

		// Extract path and params from next URL.
		// nextURL.Path may include version prefix (e.g. /v25.0/act_xxx/insights),
		// while baseURL already contains /v25.0. Remove duplicate prefix.
		path = strings.TrimPrefix(nextURL.Path, "/v25.0")
		if path == "" {
			path = "/"
		}
		params = nextURL.Query()
	}

	return nil
}

// Helper to add access token to params
func withAuth(params url.Values, token string) url.Values {
	if params == nil {
		params = url.Values{}
	}
	params.Set("access_token", token)
	return params
}

// retry executes fn up to maxRetries with exponential backoff.
func retry(maxRetries int, baseDelay time.Duration, fn func() error) error {
	var err error
	delay := baseDelay
	for attempt := 0; attempt <= maxRetries; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}
		if attempt == maxRetries {
			break
		}
		time.Sleep(delay)
		delay *= 2
	}
	return err
}
