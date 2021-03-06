package retryablehttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Client does auto-retries. If passed a logger, it logs the start and end of
// requests for observability.
type Client struct {
	c           HTTPClient
	shouldRetry ShouldRetryFn
	limiter     *rate.Limiter
	retries     int
	log         Logger
	mu          sync.RWMutex
}

type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type Logger interface {
	Printf(string, ...interface{})
}

func NewClient(client HTTPClient) *Client {
	return &Client{c: client, shouldRetry: defaultRetrier}
}

func (c *Client) WithLogger(log Logger) *Client {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.log = log
	return c
}

func (c *Client) WithRetries(r int) *Client {
	c.mu.Lock()
	defer c.mu.Unlock()

	if r < 0 {
		panic("retries must be greater than 0")
	}
	c.retries = r
	return c
}

func (c *Client) WithLimiter(l *rate.Limiter) *Client {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.limiter = l
	return c
}

// ShouldRetryFn allows specifying a custom function which should return true
// if a retry should be done, or false if no retries should be done. This
// allows you to handle specific status codes differently.
type ShouldRetryFn func(resp *http.Response) bool

func defaultRetrier(resp *http.Response) bool {
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return true
	}

	// Also retry on reverse proxy issues
	return resp.StatusCode == http.StatusBadGateway
}

func (c *Client) WithShouldRetry(fn ShouldRetryFn) *Client {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.shouldRetry = fn
	return c
}

func (c *Client) Do(req *http.Request) (*http.Response, error) {
	if c.retries == 0 {
		return c.do(req)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	var (
		rsp *http.Response
		byt []byte
		err error
	)
	if req.Body != nil {
		byt, err = ioutil.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("read body: %s", err.Error())
		}
	}
	safeURL := removeURLAuth(req.URL.String())
	header := req.Header
	ctx := req.Context()
	for i := 0; i < c.retries-1; i++ {
		rsp, err = c.retry(ctx, req, header, byt)
		if err != nil {
			if c.log != nil {
				c.log.Printf("failed with err: retry %d: %s %s", i,
					req.Method, safeURL)
			}
			time.Sleep(time.Second * time.Duration(i+1))
			continue
		}
		if c.shouldRetry(rsp) {
			if c.log != nil {
				c.log.Printf("failed with code: retry %d: %s %s",
					i, req.Method, safeURL)
				lr := io.LimitReader(rsp.Body, 1024*64)
				byt, err := ioutil.ReadAll(lr)
				if err != nil {
					c.log.Printf("failed to read resp body: %s", err)
				} else {
					c.log.Printf("response body: %s", string(byt))
				}
			}
			rsp.Body.Close()
			time.Sleep(time.Second * time.Duration(i+1))
			continue
		}
		return rsp, nil
	}
	return c.retry(ctx, req, header, byt)
}

func (c *Client) retry(
	ctx context.Context,
	req *http.Request,
	header http.Header,
	byt []byte,
) (*http.Response, error) {
	rdr := bytes.NewReader(byt)
	req, err := http.NewRequest(req.Method, req.URL.String(), rdr)
	if err != nil {
		return nil, fmt.Errorf("new request: %s", err.Error())
	}
	req = req.WithContext(ctx)
	req.Header = header
	return c.do(req)
}

// RemoveURLAuth details before logging.
func removeURLAuth(s string) string {
	ul, err := url.Parse(s)
	if err != nil {
		return ""
	}
	ul.User = nil
	return ul.String()
}

func (c *Client) do(req *http.Request) (*http.Response, error) {
	if c.log != nil {
		// Log start and end times for observability
		safeURL := removeURLAuth(req.URL.String())
		c.log.Printf("start: http: %s %s", req.Method, safeURL)
		defer c.log.Printf("end: http: %s %s", req.Method, safeURL)
	}
	if c.limiter != nil {
		if err := c.limiter.Wait(req.Context()); err != nil {
			return nil, fmt.Errorf("wait: %s", err.Error())
		}
	}
	return c.c.Do(req)
}

func retryable(statusCode int) bool {
	if statusCode >= 400 && statusCode < 500 {
		return true
	}

	// Also retry on reverse proxy issues
	return statusCode == http.StatusBadGateway
}
