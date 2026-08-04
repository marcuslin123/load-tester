package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"github.com/marcuslin123/load-tester/internal/orchestrator"
	"google.golang.org/grpc"
)

const heartbeatInterval = 3 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	address, err := parseAddress(os.Args[1:])
	if err != nil {
		log.Printf("orchestrator: %v", err)
		os.Exit(2)
	}
	if err := run(ctx, address, log.Default()); err != nil {
		log.Printf("orchestrator: %v", err)
		os.Exit(1)
	}
}

func parseAddress(args []string) (string, error) {
	flags := flag.NewFlagSet("orchestrator", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	address := flags.String("addr", ":9090", "gRPC listen address")
	if err := flags.Parse(args); err != nil {
		return "", err
	}
	if flags.NArg() != 0 {
		return "", fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	return *address, nil
}

func run(ctx context.Context, address string, logger *log.Logger) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", address, err)
	}
	defer listener.Close()

	control, err := orchestrator.NewServer(orchestrator.Options{
		HeartbeatInterval: heartbeatInterval,
		Logger:            logger,
	})
	if err != nil {
		return fmt.Errorf("create worker control server: %w", err)
	}
	server := grpc.NewServer()
	loadtestv1.RegisterWorkerControlServer(server, control)
	go func() {
		<-ctx.Done()
		server.Stop()
	}()

	logger.Printf("orchestrator listening: address=%s", listener.Addr())
	err = server.Serve(listener)
	if ctx.Err() != nil || errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return fmt.Errorf("serve gRPC: %w", err)
}
