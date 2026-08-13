package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestParseOptionsRejectsInvalidArguments(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "missing URL"},
		{name: "invalid URL", args: []string{"-url", "://bad"}},
		{name: "unsupported scheme", args: []string{"-url", "file:///tmp/health"}},
		{name: "zero timeout", args: []string{"-url", "http://example.test/healthz", "-timeout", "0s"}},
		{name: "positional argument", args: []string{"-url", "http://example.test/healthz", "extra"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseOptions(test.args); err == nil {
				t.Fatal("parseOptions() error = nil, want invalid argument error")
			}
		})
	}
}

func TestExecuteReturnsFailureForTransportError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := server.URL
	server.Close()
	var stderr bytes.Buffer

	if code := execute([]string{"-url", url}, &stderr); code == 0 {
		t.Fatalf("execute() = %d, want nonzero; stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "healthcheck:") {
		t.Fatalf("stderr = %q, want healthcheck error", stderr.String())
	}
}

func TestExecuteReturnsFailureForNon2xxResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	var stderr bytes.Buffer

	if code := execute([]string{"-url", server.URL}, &stderr); code == 0 {
		t.Fatalf("execute() = %d, want nonzero", code)
	}
	if !strings.Contains(stderr.String(), "503 Service Unavailable") {
		t.Fatalf("stderr = %q, want response status", stderr.String())
	}
}

func TestExecuteSucceedsFor2xxGET(t *testing.T) {
	requests := make(chan *http.Request, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests <- request
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	var stderr bytes.Buffer

	if code := execute([]string{"-url", server.URL, "-timeout", "1s"}, &stderr); code != 0 {
		t.Fatalf("execute() = %d, want 0; stderr = %q", code, stderr.String())
	}
	if request := <-requests; request.Method != http.MethodGet {
		t.Fatalf("method = %s, want GET", request.Method)
	}
}

func TestExecuteEnforcesRequestTimeout(t *testing.T) {
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(handlerStarted)
		<-releaseHandler
		w.WriteHeader(http.StatusNoContent)
	}))
	defer func() {
		close(releaseHandler)
		server.Close()
	}()

	var stderr bytes.Buffer
	outcomes := make(chan int, 1)
	go func() {
		outcomes <- execute([]string{"-url", server.URL, "-timeout", "20ms"}, &stderr)
	}()

	select {
	case <-handlerStarted:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("healthcheck request did not reach the test server")
	}

	select {
	case code := <-outcomes:
		if code == 0 {
			t.Fatalf("execute() = %d, want timeout failure", code)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("execute did not enforce the configured request timeout")
	}
}

func TestCheckDrainsAndClosesResponseBody(t *testing.T) {
	body := &trackingBody{Reader: strings.NewReader("response body")}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       body,
			Header:     make(http.Header),
		}, nil
	})}

	if err := check(client, "http://example.test/healthz"); err != nil {
		t.Fatalf("check() error = %v", err)
	}
	if !body.closed {
		t.Fatal("response body was not closed")
	}
	if body.Len() != 0 {
		t.Fatalf("response body has %d bytes remaining, want 0", body.Len())
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type trackingBody struct {
	*strings.Reader
	closed bool
}

func (b *trackingBody) Close() error {
	b.closed = true
	return nil
}

var _ io.ReadCloser = (*trackingBody)(nil)
