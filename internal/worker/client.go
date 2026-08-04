// Package worker owns the worker-side control-stream connection lifecycle.
package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"strings"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultRegistrationTimeout = 5 * time.Second
	defaultBaseBackoff         = 500 * time.Millisecond
	defaultMaxBackoff          = 10 * time.Second
	outboundBufferSize         = 16
)

// Config identifies this worker and the orchestrator it dials.
type Config struct {
	Address         string
	WorkerID        string
	Hostname        string
	SoftwareVersion string
}

// Options controls registration timeout, retry behavior, and diagnostics.
type Options struct {
	RegistrationTimeout time.Duration
	BaseBackoff         time.Duration
	MaxBackoff          time.Duration
	Logger              *log.Logger
}

type runtimeDependencies struct {
	dial   func(context.Context, string) (loadtestv1.WorkerControlClient, io.Closer, error)
	wait   func(context.Context, time.Duration) bool
	jitter func(time.Duration) time.Duration
	now    func() time.Time
	logger *log.Logger
}

type receiveResult struct {
	message *loadtestv1.OrchestratorMessage
	err     error
}

// Run maintains one registered stream until the caller cancels ctx or a permanent error occurs.
func Run(ctx context.Context, config Config, options Options) error {
	logger := options.Logger
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	dependencies := runtimeDependencies{
		dial: func(_ context.Context, address string) (loadtestv1.WorkerControlClient, io.Closer, error) {
			connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return nil, nil, err
			}
			return loadtestv1.NewWorkerControlClient(connection), connection, nil
		},
		wait: waitContext,
		jitter: func(limit time.Duration) time.Duration {
			if limit <= 0 {
				return 0
			}
			return time.Duration(rand.Int64N(int64(limit) + 1))
		},
		now:    time.Now,
		logger: logger,
	}
	return run(ctx, config, options, dependencies)
}

func run(ctx context.Context, config Config, options Options, dependencies runtimeDependencies) error {
	config.Address = strings.TrimSpace(config.Address)
	config.WorkerID = strings.TrimSpace(config.WorkerID)
	if config.Address == "" {
		return errors.New("orchestrator address is required")
	}
	if config.WorkerID == "" {
		return errors.New("worker ID is required")
	}
	options = applyOptionDefaults(options)
	if err := validateOptions(options); err != nil {
		return err
	}

	backoff := newRetryBackoff(options.BaseBackoff, options.MaxBackoff, dependencies.jitter)
	for {
		registered, err := runSession(ctx, config, options.RegistrationTimeout, dependencies)
		if ctx.Err() != nil {
			return nil
		}
		if isPermanent(err) {
			return fmt.Errorf("worker connection stopped: %w", err)
		}
		if registered {
			backoff.Reset()
		}
		delay := backoff.Next()
		dependencies.logger.Printf("worker connection failed: %v; retrying in %s", err, delay)
		if !dependencies.wait(ctx, delay) {
			return nil
		}
	}
}

func runSession(
	ctx context.Context,
	config Config,
	registrationTimeout time.Duration,
	dependencies runtimeDependencies,
) (bool, error) {
	client, connection, err := dependencies.dial(ctx, config.Address)
	if err != nil {
		return false, err
	}
	defer connection.Close()

	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.Connect(sessionCtx)
	if err != nil {
		return false, err
	}
	if err := stream.Send(&loadtestv1.WorkerMessage{Payload: &loadtestv1.WorkerMessage_Registration{
		Registration: &loadtestv1.Registration{
			WorkerId:        config.WorkerID,
			Hostname:        config.Hostname,
			SoftwareVersion: config.SoftwareVersion,
		},
	}}); err != nil {
		return false, err
	}

	ackMessage, err := receiveWithTimeout(sessionCtx, cancel, stream, registrationTimeout)
	if err != nil {
		return false, err
	}
	ack := ackMessage.GetRegistrationAck()
	if ack == nil {
		return false, status.Error(codes.FailedPrecondition, "registration acknowledgment must be the first orchestrator message")
	}
	if ack.GetWorkerId() != config.WorkerID {
		return false, status.Errorf(codes.FailedPrecondition, "registration acknowledged worker %q, want %q", ack.GetWorkerId(), config.WorkerID)
	}
	if ack.GetHeartbeatInterval() == nil {
		return false, status.Error(codes.FailedPrecondition, "heartbeat interval is required")
	}
	if err := ack.GetHeartbeatInterval().CheckValid(); err != nil {
		return false, status.Errorf(codes.FailedPrecondition, "invalid heartbeat interval: %v", err)
	}
	heartbeatInterval := ack.GetHeartbeatInterval().AsDuration()
	if heartbeatInterval <= 0 {
		return false, status.Error(codes.FailedPrecondition, "heartbeat interval must be greater than zero")
	}
	dependencies.logger.Printf("worker registered: id=%s heartbeat_interval=%s", config.WorkerID, heartbeatInterval)

	err = runRegistered(sessionCtx, cancel, stream, config.WorkerID, heartbeatInterval, dependencies)
	return true, err
}

func receiveWithTimeout(
	ctx context.Context,
	cancel context.CancelFunc,
	stream loadtestv1.WorkerControl_ConnectClient,
	timeout time.Duration,
) (*loadtestv1.OrchestratorMessage, error) {
	result := make(chan receiveResult, 1)
	go func() {
		message, err := stream.Recv()
		result <- receiveResult{message: message, err: err}
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case received := <-result:
		return received.message, received.err
	case <-timer.C:
		cancel()
		return nil, status.Error(codes.DeadlineExceeded, "registration acknowledgment timed out")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func runRegistered(
	ctx context.Context,
	cancel context.CancelFunc,
	stream loadtestv1.WorkerControl_ConnectClient,
	workerID string,
	heartbeatInterval time.Duration,
	dependencies runtimeDependencies,
) error {
	outbound := make(chan *loadtestv1.WorkerMessage, outboundBufferSize)
	errors := make(chan error, 2)
	assignments := newAssignmentState()
	go writeMessages(ctx, stream, outbound, errors)
	go produceHeartbeats(ctx, workerID, heartbeatInterval, dependencies.now, assignments, outbound)
	go receiveMessages(ctx, stream, assignments, dependencies.logger, errors)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errors:
		cancel()
		return err
	}
}

// writeMessages is the stream's only sender; later metric deltas share this queue.
func writeMessages(
	ctx context.Context,
	stream loadtestv1.WorkerControl_ConnectClient,
	outbound <-chan *loadtestv1.WorkerMessage,
	errors chan<- error,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-outbound:
			if err := stream.Send(message); err != nil {
				reportStreamError(ctx, errors, err)
				return
			}
		}
	}
}

func produceHeartbeats(
	ctx context.Context,
	workerID string,
	interval time.Duration,
	now func() time.Time,
	assignments *assignmentState,
	outbound chan<- *loadtestv1.WorkerMessage,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for sequence := uint64(1); ; sequence++ {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runID, revision := assignments.Status()
			message := &loadtestv1.WorkerMessage{Payload: &loadtestv1.WorkerMessage_Heartbeat{
				Heartbeat: &loadtestv1.Heartbeat{
					WorkerId:                  workerID,
					Sequence:                  sequence,
					SentAt:                    timestamppb.New(now()),
					State:                     loadtestv1.WorkerState_WORKER_STATE_IDLE,
					ActiveRunId:               runID,
					AppliedAssignmentRevision: revision,
				},
			}}
			select {
			case outbound <- message:
			case <-ctx.Done():
				return
			}
		}
	}
}

func receiveMessages(
	ctx context.Context,
	stream loadtestv1.WorkerControl_ConnectClient,
	assignments *assignmentState,
	logger *log.Logger,
	errors chan<- error,
) {
	for {
		message, err := stream.Recv()
		if err != nil {
			reportStreamError(ctx, errors, err)
			return
		}
		assignment := message.GetAssignment()
		if assignment == nil {
			reportStreamError(ctx, errors, status.Error(codes.FailedPrecondition, "only load assignments are accepted after registration"))
			return
		}
		if err := assignments.Apply(assignment); err != nil {
			reportStreamError(ctx, errors, err)
			return
		}
		logger.Printf("assignment applied: run_id=%s revision=%d", assignment.GetRunId(), assignment.GetRevision())
	}
}

func reportStreamError(ctx context.Context, errors chan<- error, err error) {
	if ctx.Err() != nil {
		return
	}
	select {
	case errors <- err:
	case <-ctx.Done():
	}
}

func applyOptionDefaults(options Options) Options {
	if options.RegistrationTimeout == 0 {
		options.RegistrationTimeout = defaultRegistrationTimeout
	}
	if options.BaseBackoff == 0 {
		options.BaseBackoff = defaultBaseBackoff
	}
	if options.MaxBackoff == 0 {
		options.MaxBackoff = defaultMaxBackoff
	}
	return options
}

func validateOptions(options Options) error {
	if options.RegistrationTimeout <= 0 {
		return errors.New("registration timeout must be greater than zero")
	}
	if options.BaseBackoff <= 0 {
		return errors.New("base backoff must be greater than zero")
	}
	if options.MaxBackoff < options.BaseBackoff {
		return errors.New("maximum backoff must be greater than or equal to base backoff")
	}
	return nil
}

func isPermanent(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.Unauthenticated, codes.PermissionDenied:
		return true
	default:
		return false
	}
}

func waitContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

type retryBackoff struct {
	base    time.Duration
	maximum time.Duration
	current time.Duration
	jitter  func(time.Duration) time.Duration
}

func newRetryBackoff(base, maximum time.Duration, jitter func(time.Duration) time.Duration) *retryBackoff {
	return &retryBackoff{base: base, maximum: maximum, current: base, jitter: jitter}
}

func (b *retryBackoff) Next() time.Duration {
	delay := b.jitter(b.current)
	if b.current >= b.maximum/2 {
		b.current = b.maximum
	} else {
		b.current *= 2
	}
	return delay
}

func (b *retryBackoff) Reset() {
	b.current = b.base
}
