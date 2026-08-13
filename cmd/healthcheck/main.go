package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"
)

const defaultTimeout = 2 * time.Second

type cliOptions struct {
	url     string
	timeout time.Duration
}

func main() {
	os.Exit(execute(os.Args[1:], os.Stderr))
}

func execute(args []string, stderr io.Writer) int {
	options, err := parseOptions(args)
	if err == nil {
		client := &http.Client{Timeout: options.timeout}
		err = check(client, options.url)
	}
	if err != nil {
		fmt.Fprintf(stderr, "healthcheck: %v\n", err)
		return 1
	}
	return 0
}

func parseOptions(args []string) (cliOptions, error) {
	flags := flag.NewFlagSet("healthcheck", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	checkURL := flags.String("url", "", "health-check URL")
	timeout := flags.Duration("timeout", defaultTimeout, "request timeout")
	if err := flags.Parse(args); err != nil {
		return cliOptions{}, err
	}
	if flags.NArg() != 0 {
		return cliOptions{}, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if *checkURL == "" {
		return cliOptions{}, errors.New("-url is required")
	}
	parsedURL, err := url.ParseRequestURI(*checkURL)
	if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" {
		return cliOptions{}, fmt.Errorf("invalid -url %q", *checkURL)
	}
	if *timeout <= 0 {
		return cliOptions{}, errors.New("-timeout must be greater than zero")
	}
	return cliOptions{url: *checkURL, timeout: *timeout}, nil
}

func check(client *http.Client, checkURL string) error {
	request, err := http.NewRequest(http.MethodGet, checkURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("GET %s: %w", checkURL, err)
	}

	_, drainErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if drainErr != nil {
		return fmt.Errorf("drain response body: %w", drainErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close response body: %w", closeErr)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("unexpected HTTP status: %s", response.Status)
	}
	return nil
}
