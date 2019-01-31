package retryablehttp

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

// Client does auto-retries. If passed a logger, it logs the start and end of
// requests for observability.
type Client struct {
	c       *http.Client
	retries int
	log     Logger
	mu      sync.RWMutex
}

type Logger interface {
	Printf(string, ...interface{})
}

func NewClient(client *http.Client) *Client {
	return &Client{c: client}
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

func (c *Client) Do(req *http.Request) (*http.Response, error) {
	if c.retries == 0 {
		return c.c.Do(req)
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
			return nil, err
		}
	}
	ctx := req.Context()
	for i := 0; i < c.retries; i++ {
		rsp, err = c.retry(ctx, req, byt)
		if err != nil {
			time.Sleep(time.Second * time.Duration(i+1))
			continue
		}
		if retryable(rsp.StatusCode) {
			rsp.Body.Close()
			time.Sleep(time.Second * time.Duration(i+1))
			continue
		}
		return rsp, nil
	}
	return rsp, err
}

func (c *Client) retry(
	ctx context.Context,
	req *http.Request,
	byt []byte,
) (*http.Response, error) {
	rdr := bytes.NewReader(byt)
	req, err := http.NewRequest(req.Method, req.URL.String(), rdr)
	if err != nil {
		return nil, err
	}
	return c.c.Do(req)
}

func retryable(statusCode int) bool {
	if statusCode <= 400 && statusCode < 500 {
		return true
	}

	// Also retry on reverse proxy issues
	return statusCode == http.StatusBadGateway
}
