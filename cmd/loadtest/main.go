package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/marcuslin123/load-tester/internal/config"
	"github.com/marcuslin123/load-tester/internal/protocol"
	"github.com/marcuslin123/load-tester/internal/report"
	"github.com/marcuslin123/load-tester/internal/runner"
)

const defaultPreflightTimeout = 5 * time.Second

type exitCode int

const (
	exitPass         exitCode = 0
	exitTestFailure  exitCode = 1
	exitSetupFailure exitCode = 2
	exitInterrupted  exitCode = 130
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	os.Exit(int(run(ctx, os.Args[1:], os.Stdout, os.Stderr)))
}

// run keeps process-only concerns outside the reusable runner and returns the intended exit status.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) exitCode {
	flags := flag.NewFlagSet("loadtest", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "path to the load-test YAML file")
	preflightTimeout := flags.Duration("preflight-timeout", defaultPreflightTimeout, "maximum time for the unmeasured reachability request")
	if err := flags.Parse(args); err != nil {
		return exitSetupFailure
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "loadtest: unexpected arguments: %v\n", flags.Args())
		return exitSetupFailure
	}
	if *configPath == "" {
		fmt.Fprintln(stderr, "loadtest: -config is required")
		return exitSetupFailure
	}
	if *preflightTimeout <= 0 {
		fmt.Fprintln(stderr, "loadtest: -preflight-timeout must be greater than zero")
		return exitSetupFailure
	}

	file, err := os.Open(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "loadtest: open config: %v\n", err)
		return exitSetupFailure
	}
	cfg, parseErr := config.Parse(file)
	closeErr := file.Close()
	if parseErr != nil {
		fmt.Fprintf(stderr, "loadtest: %v\n", parseErr)
		return exitSetupFailure
	}
	if closeErr != nil {
		fmt.Fprintf(stderr, "loadtest: close config: %v\n", closeErr)
		return exitSetupFailure
	}

	executor, err := protocol.NewHTTP(cfg.Target)
	if err != nil {
		fmt.Fprintf(stderr, "loadtest: configure protocol: %v\n", err)
		return exitSetupFailure
	}
	summary, err := runner.Run(ctx, cfg, executor, runner.Options{
		PreflightTimeout: *preflightTimeout,
	})
	if err != nil {
		fmt.Fprintf(stderr, "loadtest: %v\n", err)
		if ctx.Err() != nil {
			return exitInterrupted
		}
		return exitSetupFailure
	}
	if err := report.WriteText(stdout, summary); err != nil {
		fmt.Fprintf(stderr, "loadtest: %v\n", err)
		return exitSetupFailure
	}

	switch summary.Status {
	case report.StatusPass:
		return exitPass
	case report.StatusFail:
		return exitTestFailure
	case report.StatusInterrupted:
		return exitInterrupted
	default:
		fmt.Fprintf(stderr, "loadtest: unknown result status %q\n", summary.Status)
		return exitSetupFailure
	}
}
