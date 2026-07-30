package targetapp

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHealthReturnsOK(t *testing.T) {
	recorder := serveRequest("/health")

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if recorder.Body.String() != "ok\n" {
		t.Fatalf("body = %q, want %q", recorder.Body.String(), "ok\n")
	}
}

func TestEchoWaitsForInjectedLatency(t *testing.T) {
	started := time.Now()
	recorder := serveRequest("/echo?latency=20ms")
	elapsed := time.Since(started)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if elapsed < 15*time.Millisecond {
		t.Fatalf("elapsed = %s, want at least 15ms", elapsed)
	}
}

func TestEchoAlwaysFailsAtErrorRateOne(t *testing.T) {
	for range 10 {
		recorder := serveRequest("/echo?error_rate=1.0")
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
		}
	}
}

func TestEchoRejectsInvalidParameters(t *testing.T) {
	tests := []string{
		"/echo?latency=",
		"/echo?latency=1ms&latency=2ms",
		"/echo?latency=slow",
		"/echo?latency=-1ms",
		"/echo?latency=11s",
		"/echo?error_rate=",
		"/echo?error_rate=0&error_rate=1",
		"/echo?error_rate=often",
		"/echo?error_rate=NaN",
		"/echo?error_rate=-0.1",
		"/echo?error_rate=1.1",
	}

	for _, path := range tests {
		t.Run(path, func(t *testing.T) {
			recorder := serveRequest(path)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestBurnConsumesRequestedTime(t *testing.T) {
	started := time.Now()
	recorder := serveRequest("/burn?ms=20")
	elapsed := time.Since(started)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if elapsed < 15*time.Millisecond {
		t.Fatalf("elapsed = %s, want at least 15ms", elapsed)
	}
}

func TestBurnRejectsInvalidMilliseconds(t *testing.T) {
	tests := []string{
		"/burn",
		"/burn?ms=1&ms=2",
		"/burn?ms=lots",
		"/burn?ms=-1",
		"/burn?ms=10001",
	}

	for _, path := range tests {
		t.Run(path, func(t *testing.T) {
			recorder := serveRequest(path)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestHandlerServesConcurrentRequests(t *testing.T) {
	handler := NewHandler()
	results := make(chan int, 50)

	for range 50 {
		go func() {
			request := httptest.NewRequest(http.MethodGet, "/echo?error_rate=0", nil)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			results <- recorder.Code
		}()
	}

	for range 50 {
		if status := <-results; status != http.StatusOK {
			t.Fatalf("status = %d, want %d", status, http.StatusOK)
		}
	}
}

func serveRequest(path string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, path, nil)
	recorder := httptest.NewRecorder()
	NewHandler().ServeHTTP(recorder, request)
	return recorder
}
