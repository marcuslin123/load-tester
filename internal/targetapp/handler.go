package targetapp

import (
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

const maxInjectedDuration = 10 * time.Second

// NewHandler returns the target application's HTTP routes.
func NewHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", health)
	mux.HandleFunc("GET /echo", echo)
	mux.HandleFunc("GET /burn", burn)
	return mux
}

func health(w http.ResponseWriter, _ *http.Request) {
	fmt.Fprintln(w, "ok")
}

func echo(w http.ResponseWriter, r *http.Request) {
	latencyValue, err := queryValue(r, "latency", false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	latency, err := parseLatency(latencyValue)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	errorRateValue, err := queryValue(r, "error_rate", false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	errorRate, err := parseErrorRate(errorRateValue)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if !wait(r, latency) {
		return
	}

	if rand.Float64() < errorRate {
		http.Error(w, "injected error", http.StatusServiceUnavailable)
		return
	}

	fmt.Fprintln(w, "ok")
}

func burn(w http.ResponseWriter, r *http.Request) {
	millisecondsValue, err := queryValue(r, "ms", true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	milliseconds, err := parseBurnMilliseconds(millisecondsValue)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	deadline := time.Now().Add(time.Duration(milliseconds) * time.Millisecond)
	for time.Now().Before(deadline) {
		select {
		case <-r.Context().Done():
			return
		default:
		}
	}

	fmt.Fprintln(w, "ok")
}

func queryValue(r *http.Request, name string, required bool) (string, error) {
	values, present := r.URL.Query()[name]
	if !present {
		if required {
			return "", fmt.Errorf("%s is required", name)
		}
		return "", nil
	}
	if len(values) != 1 || values[0] == "" {
		return "", fmt.Errorf("%s must be specified exactly once", name)
	}
	return values[0], nil
}

func parseLatency(value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}

	latency, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid latency: %q", value)
	}
	if latency < 0 || latency > maxInjectedDuration {
		return 0, fmt.Errorf("latency must be between 0s and %s", maxInjectedDuration)
	}
	return latency, nil
}

func parseErrorRate(value string) (float64, error) {
	if value == "" {
		return 0, nil
	}

	rate, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(rate) || rate < 0 || rate > 1 {
		return 0, fmt.Errorf("error_rate must be a number between 0 and 1")
	}
	return rate, nil
}

func parseBurnMilliseconds(value string) (int, error) {
	milliseconds, err := strconv.Atoi(value)
	maxMilliseconds := int(maxInjectedDuration / time.Millisecond)
	if err != nil || milliseconds < 0 || milliseconds > maxMilliseconds {
		return 0, fmt.Errorf("ms must be an integer between 0 and %d", maxMilliseconds)
	}
	return milliseconds, nil
}

func wait(r *http.Request, duration time.Duration) bool {
	if duration == 0 {
		return true
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-r.Context().Done():
		return false
	}
}
