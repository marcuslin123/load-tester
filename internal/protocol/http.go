package protocol

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/marcuslin123/load-tester/internal/config"
)

const maxIdleConnections = 1_000

// HTTPProtocol executes a configured HTTP request using a reusable client.
type HTTPProtocol struct {
	client  *http.Client
	method  string
	url     string
	headers http.Header
	body    []byte
}

// NewHTTP creates an HTTP adapter from a validated target configuration.
func NewHTTP(target config.Target) (*HTTPProtocol, error) {
	if _, err := http.NewRequest(target.Method, target.URL, bytes.NewReader([]byte(target.Body))); err != nil {
		return nil, fmt.Errorf("create HTTP request: %w", err)
	}

	headers := make(http.Header, len(target.Headers))
	for name, value := range target.Headers {
		headers.Set(name, value)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = maxIdleConnections
	transport.MaxIdleConnsPerHost = maxIdleConnections

	return &HTTPProtocol{
		client:  &http.Client{Transport: transport},
		method:  target.Method,
		url:     target.URL,
		headers: headers,
		body:    []byte(target.Body),
	}, nil
}

// Execute sends one request and measures through the end of its response body.
func (p *HTTPProtocol) Execute(ctx context.Context) Result {
	request, err := http.NewRequestWithContext(ctx, p.method, p.url, bytes.NewReader(p.body))
	if err != nil {
		return Result{Err: fmt.Errorf("create HTTP request: %w", err)}
	}
	request.Header = p.headers.Clone()

	started := time.Now()
	response, err := p.client.Do(request)
	if err != nil {
		return Result{
			Latency: time.Since(started),
			Err:     fmt.Errorf("execute HTTP request: %w", err),
		}
	}

	bytesRead, readErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	result := Result{
		Latency:    time.Since(started),
		StatusCode: response.StatusCode,
		BytesRead:  bytesRead,
	}
	if readErr != nil {
		result.Err = fmt.Errorf("read HTTP response body: %w", readErr)
	} else if closeErr != nil {
		result.Err = fmt.Errorf("close HTTP response body: %w", closeErr)
	}
	return result
}
