package protocol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcuslin123/load-tester/internal/config"
	"github.com/marcuslin123/load-tester/internal/targetapp"
)

func TestHTTPProtocolExecutesConfiguredRequest(t *testing.T) {
	type receivedRequest struct {
		method string
		header string
		body   string
	}
	received := make(chan receivedRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			return
		}
		received <- receivedRequest{
			method: r.Method,
			header: r.Header.Get("Content-Type"),
			body:   string(body),
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, "created")
	}))
	defer server.Close()

	adapter, err := NewHTTP(config.Target{
		URL:    server.URL,
		Method: http.MethodPost,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: `{"item_id":42}`,
	})
	if err != nil {
		t.Fatalf("NewHTTP() error = %v", err)
	}
	var _ Protocol = adapter

	result := adapter.Execute(context.Background())
	if result.Err != nil {
		t.Fatalf("Execute() error = %v", result.Err)
	}
	if result.StatusCode != http.StatusCreated {
		t.Fatalf("StatusCode = %d, want %d", result.StatusCode, http.StatusCreated)
	}
	if result.BytesRead != int64(len("created")) {
		t.Fatalf("BytesRead = %d, want %d", result.BytesRead, len("created"))
	}
	if result.Latency <= 0 {
		t.Fatalf("Latency = %s, want greater than 0", result.Latency)
	}

	select {
	case request := <-received:
		if request.method != http.MethodPost {
			t.Errorf("method = %q, want POST", request.method)
		}
		if request.header != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", request.header)
		}
		if request.body != `{"item_id":42}` {
			t.Errorf("body = %q, want configured JSON", request.body)
		}
	case <-time.After(time.Second):
		t.Fatal("target did not receive request")
	}
}

func TestHTTPProtocolMeasuresThroughCompleteResponseBody(t *testing.T) {
	const bodyDelay = 50 * time.Millisecond
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "first")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(bodyDelay)
		_, _ = io.WriteString(w, "second")
	}))
	defer server.Close()

	adapter := mustNewHTTP(t, config.Target{URL: server.URL, Method: http.MethodGet})
	result := adapter.Execute(context.Background())

	if result.Err != nil {
		t.Fatalf("Execute() error = %v", result.Err)
	}
	if result.Latency < bodyDelay {
		t.Fatalf("Latency = %s, want at least %s", result.Latency, bodyDelay)
	}
	if result.BytesRead != int64(len("firstsecond")) {
		t.Fatalf("BytesRead = %d, want %d", result.BytesRead, len("firstsecond"))
	}
}

func TestHTTPProtocolMeasuresInjectedTargetLatency(t *testing.T) {
	const injectedLatency = 40 * time.Millisecond
	server := httptest.NewServer(targetapp.NewHandler())
	defer server.Close()

	adapter := mustNewHTTP(t, config.Target{
		URL:    server.URL + "/echo?latency=" + injectedLatency.String(),
		Method: http.MethodGet,
	})
	result := adapter.Execute(context.Background())

	if result.Err != nil {
		t.Fatalf("Execute() error = %v", result.Err)
	}
	if result.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, want %d", result.StatusCode, http.StatusOK)
	}
	if result.Latency < injectedLatency {
		t.Fatalf("Latency = %s, want at least injected %s", result.Latency, injectedLatency)
	}
}

func TestHTTPProtocolReturnsHTTPFailureAsResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "unavailable")
	}))
	defer server.Close()

	adapter := mustNewHTTP(t, config.Target{URL: server.URL, Method: http.MethodGet})
	result := adapter.Execute(context.Background())

	if result.Err != nil {
		t.Fatalf("Execute() error = %v, want nil for HTTP response", result.Err)
	}
	if result.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("StatusCode = %d, want %d", result.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestHTTPProtocolReturnsTransportError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	targetURL := server.URL
	server.Close()

	adapter := mustNewHTTP(t, config.Target{URL: targetURL, Method: http.MethodGet})
	result := adapter.Execute(context.Background())

	if result.Err == nil {
		t.Fatal("Execute() error = nil, want connection error")
	}
	if result.StatusCode != 0 {
		t.Fatalf("StatusCode = %d, want 0 without HTTP response", result.StatusCode)
	}
}

func TestHTTPProtocolStopsOnContextCancellation(t *testing.T) {
	requestStarted := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		<-r.Context().Done()
	}))
	defer server.Close()

	adapter := mustNewHTTP(t, config.Target{URL: server.URL, Method: http.MethodGet})
	ctx, cancel := context.WithCancel(context.Background())
	resultReady := make(chan Result, 1)
	go func() {
		resultReady <- adapter.Execute(ctx)
	}()

	select {
	case <-requestStarted:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("request did not reach target")
	}

	select {
	case result := <-resultReady:
		if !errors.Is(result.Err, context.Canceled) {
			t.Fatalf("Execute() error = %v, want context canceled", result.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("Execute() did not stop after cancellation")
	}
}

func TestHTTPProtocolReportsIncompleteResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		connection, buffer, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack connection: %v", err)
			return
		}
		defer connection.Close()
		_, _ = fmt.Fprint(buffer, "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort")
		_ = buffer.Flush()
	}))
	defer server.Close()

	adapter := mustNewHTTP(t, config.Target{URL: server.URL, Method: http.MethodGet})
	result := adapter.Execute(context.Background())

	if result.Err == nil {
		t.Fatal("Execute() error = nil, want incomplete body error")
	}
	if result.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, want %d", result.StatusCode, http.StatusOK)
	}
	if result.BytesRead != int64(len("short")) {
		t.Fatalf("BytesRead = %d, want %d", result.BytesRead, len("short"))
	}
}

func TestHTTPProtocolUsesIndependentBodiesConcurrently(t *testing.T) {
	const (
		requestCount = 20
		requestBody  = `{"item_id":42}`
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != requestBody {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()

	adapter := mustNewHTTP(t, config.Target{
		URL:    server.URL,
		Method: http.MethodPost,
		Body:   requestBody,
	})
	results := executeConcurrently(adapter, requestCount)
	for i, result := range results {
		if result.Err != nil {
			t.Fatalf("result %d error = %v", i, result.Err)
		}
		if result.StatusCode != http.StatusOK {
			t.Fatalf("result %d status = %d, want 200", i, result.StatusCode)
		}
	}
}

func TestHTTPProtocolRetainsConnectionsForReuse(t *testing.T) {
	const waveSize = 10
	waveOneReady := make(chan struct{})
	waveOneRelease := make(chan struct{})
	waveTwoReady := make(chan struct{})
	waveTwoRelease := make(chan struct{})
	var requests atomic.Int64

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestNumber := requests.Add(1)
		switch {
		case requestNumber <= waveSize:
			if requestNumber == waveSize {
				close(waveOneReady)
			}
			<-waveOneRelease
		case requestNumber <= 2*waveSize:
			if requestNumber == 2*waveSize {
				close(waveTwoReady)
			}
			<-waveTwoRelease
		}
		_, _ = io.WriteString(w, "ok")
	}))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	counting := &countingListener{Listener: listener}
	server.Listener = counting
	server.Start()
	defer server.Close()

	adapter := mustNewHTTP(t, config.Target{URL: server.URL, Method: http.MethodGet})
	waveOneDone := executeConcurrentlyAsync(adapter, waveSize)
	waitForSignal(t, waveOneReady, "first request wave")
	close(waveOneRelease)
	resultsOne := <-waveOneDone
	assertSuccessfulResults(t, resultsOne)
	connectionsAfterWaveOne := counting.accepted.Load()

	waveTwoDone := executeConcurrentlyAsync(adapter, waveSize)
	waitForSignal(t, waveTwoReady, "second request wave")
	close(waveTwoRelease)
	resultsTwo := <-waveTwoDone
	assertSuccessfulResults(t, resultsTwo)

	if got := counting.accepted.Load(); got > connectionsAfterWaveOne+1 {
		t.Fatalf("accepted connections = %d after starting with %d, want existing pool reused", got, connectionsAfterWaveOne)
	}
}

func TestHTTPProtocolReusesConnectionAcrossSequentialRequests(t *testing.T) {
	const requestCount = 20
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	counting := &countingListener{Listener: listener}
	server.Listener = counting
	server.Start()
	defer server.Close()

	adapter := mustNewHTTP(t, config.Target{URL: server.URL, Method: http.MethodGet})
	for i := range requestCount {
		result := adapter.Execute(context.Background())
		if result.Err != nil || result.StatusCode != http.StatusOK {
			t.Fatalf("request %d = %+v, want successful response", i, result)
		}
	}

	if got := counting.accepted.Load(); got > 2 {
		t.Fatalf("accepted connections = %d for %d sequential requests, want at most 2", got, requestCount)
	}
}

func TestNewHTTPRejectsInvalidTarget(t *testing.T) {
	_, err := NewHTTP(config.Target{Method: http.MethodGet, URL: "://bad-url"})
	if err == nil {
		t.Fatal("NewHTTP() error = nil, want invalid URL error")
	}
}

type countingListener struct {
	net.Listener
	accepted atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
	}
	return connection, err
}

func mustNewHTTP(t *testing.T, target config.Target) *HTTPProtocol {
	t.Helper()
	adapter, err := NewHTTP(target)
	if err != nil {
		t.Fatalf("NewHTTP() error = %v", err)
	}
	return adapter
}

func executeConcurrently(adapter *HTTPProtocol, count int) []Result {
	return <-executeConcurrentlyAsync(adapter, count)
}

func executeConcurrentlyAsync(adapter *HTTPProtocol, count int) <-chan []Result {
	done := make(chan []Result, 1)
	go func() {
		results := make([]Result, count)
		var group sync.WaitGroup
		group.Add(count)
		for i := range count {
			go func() {
				defer group.Done()
				results[i] = adapter.Execute(context.Background())
			}()
		}
		group.Wait()
		done <- results
	}()
	return done
}

func waitForSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func assertSuccessfulResults(t *testing.T, results []Result) {
	t.Helper()
	for i, result := range results {
		if result.Err != nil || result.StatusCode != http.StatusOK {
			t.Fatalf("result %d = %+v, want successful response", i, result)
		}
	}
}
