package loadtestv1_test

import (
	"reflect"
	"testing"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestWorkerControlConnectIsBidirectional(t *testing.T) {
	t.Parallel()

	if got, want := loadtestv1.WorkerControl_Connect_FullMethodName, "/loadtest.v1.WorkerControl/Connect"; got != want {
		t.Fatalf("full method name = %q, want %q", got, want)
	}
	service := loadtestv1.File_loadtest_v1_loadtest_proto.Services().ByName("WorkerControl")
	if service == nil {
		t.Fatal("WorkerControl service is missing")
	}
	methods := service.Methods()
	if methods.Len() != 1 || methods.Get(0).Name() != "Connect" {
		t.Fatalf("WorkerControl methods = %d, want one Connect method", methods.Len())
	}
	connect := methods.Get(0)
	if !connect.IsStreamingClient() || !connect.IsStreamingServer() {
		t.Fatalf("Connect streams = client:%t server:%t, want bidirectional", connect.IsStreamingClient(), connect.IsStreamingServer())
	}
}

func TestEnvelopeMessagesRoundTrip(t *testing.T) {
	t.Parallel()

	started := timestamppb.New(time.Unix(1_700_000_000, 0))
	deadline := timestamppb.New(time.Unix(1_700_000_060, 0))
	tests := []struct {
		name    string
		message proto.Message
		new     func() proto.Message
	}{
		{
			name: "registration",
			message: &loadtestv1.WorkerMessage{Payload: &loadtestv1.WorkerMessage_Registration{
				Registration: &loadtestv1.Registration{
					WorkerId:        "worker-1",
					Hostname:        "worker-1.local",
					SoftwareVersion: "0.1.0",
				},
			}},
			new: func() proto.Message { return &loadtestv1.WorkerMessage{} },
		},
		{
			name: "metrics",
			message: &loadtestv1.WorkerMessage{Payload: &loadtestv1.WorkerMessage_Metrics{
				Metrics: &loadtestv1.MetricsDelta{
					WorkerId:           "worker-1",
					RunId:              "run-7",
					AssignmentRevision: 3,
					Sequence:           11,
					IntervalStart:      started,
					IntervalEnd:        deadline,
					Counters: &loadtestv1.MetricCounters{
						Requests:       100,
						Succeeded:      90,
						Failed:         10,
						ServerErrors:   10,
						BytesRead:      4096,
						DroppedSamples: 2,
						UnmetDemand:    4,
						StatusCodes:    map[int32]uint64{200: 90, 503: 10},
					},
					HistogramEncoding: loadtestv1.HistogramEncoding_HISTOGRAM_ENCODING_HDR_V2_COMPRESSED,
					LatencyHistogram:  []byte{0x01, 0x02, 0x03},
				},
			}},
			new: func() proto.Message { return &loadtestv1.WorkerMessage{} },
		},
		{
			name: "heartbeat",
			message: &loadtestv1.WorkerMessage{Payload: &loadtestv1.WorkerMessage_Heartbeat{
				Heartbeat: &loadtestv1.Heartbeat{
					WorkerId:                  "worker-1",
					Sequence:                  12,
					SentAt:                    started,
					State:                     loadtestv1.WorkerState_WORKER_STATE_RUNNING,
					ActiveRunId:               "run-7",
					AppliedAssignmentRevision: 3,
					InFlightRequests:          20,
				},
			}},
			new: func() proto.Message { return &loadtestv1.WorkerMessage{} },
		},
		{
			name: "registration acknowledgment",
			message: &loadtestv1.OrchestratorMessage{Payload: &loadtestv1.OrchestratorMessage_RegistrationAck{
				RegistrationAck: &loadtestv1.RegistrationAck{
					WorkerId:          "worker-1",
					HeartbeatInterval: durationpb.New(3 * time.Second),
				},
			}},
			new: func() proto.Message { return &loadtestv1.OrchestratorMessage{} },
		},
		{
			name: "load assignment",
			message: &loadtestv1.OrchestratorMessage{Payload: &loadtestv1.OrchestratorMessage_Assignment{
				Assignment: &loadtestv1.LoadAssignment{
					RunId:    "run-7",
					Revision: 3,
					Target: &loadtestv1.Target{Protocol: &loadtestv1.Target_Http{
						Http: &loadtestv1.HttpTarget{
							Url:     "http://target:8080/echo",
							Method:  "POST",
							Headers: map[string]string{"Content-Type": "application/json"},
							Body:    []byte(`{"item_id":42}`),
						},
					}},
					Load: &loadtestv1.LoadSlice{
						Model:       loadtestv1.LoadModel_LOAD_MODEL_CONSTANT_RATE,
						Rate:        500,
						MaxInFlight: 1_000,
						RampUp:      durationpb.New(10 * time.Second),
					},
					StartsAt: started,
					Deadline: deadline,
				},
			}},
			new: func() proto.Message { return &loadtestv1.OrchestratorMessage{} },
		},
		{
			name: "stop command",
			message: &loadtestv1.OrchestratorMessage{Payload: &loadtestv1.OrchestratorMessage_Stop{
				Stop: &loadtestv1.StopCommand{
					RunId:  "run-7",
					Reason: loadtestv1.StopReason_STOP_REASON_CANCELED,
				},
			}},
			new: func() proto.Message { return &loadtestv1.OrchestratorMessage{} },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			encoded, err := proto.Marshal(test.message)
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}
			decoded := test.new()
			if err := proto.Unmarshal(encoded, decoded); err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}
			if !proto.Equal(decoded, test.message) {
				t.Errorf("round trip = %v, want %v", decoded, test.message)
			}
		})
	}
}

func TestWireFieldNumbersRemainStable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		message protoreflect.MessageDescriptor
		fields  map[string]protoreflect.FieldNumber
	}{
		{
			message: (&loadtestv1.WorkerMessage{}).ProtoReflect().Descriptor(),
			fields:  map[string]protoreflect.FieldNumber{"registration": 1, "metrics": 2, "heartbeat": 3},
		},
		{
			message: (&loadtestv1.OrchestratorMessage{}).ProtoReflect().Descriptor(),
			fields:  map[string]protoreflect.FieldNumber{"registration_ack": 1, "assignment": 2, "stop": 3},
		},
		{
			message: (&loadtestv1.LoadAssignment{}).ProtoReflect().Descriptor(),
			fields: map[string]protoreflect.FieldNumber{
				"run_id": 1, "revision": 2, "target": 3, "load": 4, "starts_at": 5, "deadline": 6,
			},
		},
		{
			message: (&loadtestv1.MetricsDelta{}).ProtoReflect().Descriptor(),
			fields: map[string]protoreflect.FieldNumber{
				"worker_id": 1, "run_id": 2, "assignment_revision": 3, "sequence": 4,
				"interval_start": 5, "interval_end": 6, "counters": 7,
				"histogram_encoding": 8, "latency_histogram": 9,
			},
		},
	}

	for _, test := range tests {
		for name, want := range test.fields {
			field := test.message.Fields().ByName(protoreflect.Name(name))
			if field == nil {
				t.Errorf("%s.%s is missing", test.message.FullName(), name)
				continue
			}
			if got := field.Number(); got != want {
				t.Errorf("%s.%s field number = %d, want %d", test.message.FullName(), name, got, want)
			}
		}
	}
}

func TestEnvelopesUseOneofPayloads(t *testing.T) {
	t.Parallel()

	for _, message := range []protoreflect.MessageDescriptor{
		(&loadtestv1.WorkerMessage{}).ProtoReflect().Descriptor(),
		(&loadtestv1.OrchestratorMessage{}).ProtoReflect().Descriptor(),
	} {
		oneofs := message.Oneofs()
		if oneofs.Len() != 1 || oneofs.Get(0).Name() != "payload" {
			t.Errorf("%s oneofs = %v, want one payload oneof", message.FullName(), oneofNames(oneofs))
		}
	}
}

func TestTargetUsesProtocolOneof(t *testing.T) {
	t.Parallel()

	target := (&loadtestv1.Target{}).ProtoReflect().Descriptor()
	oneofs := target.Oneofs()
	if oneofs.Len() != 1 || oneofs.Get(0).Name() != "protocol" {
		t.Fatalf("Target oneofs = %v, want one protocol oneof", oneofNames(oneofs))
	}
	httpField := target.Fields().ByName("http")
	if httpField == nil || httpField.Number() != 1 {
		t.Fatalf("Target.http = %v, want stable field number 1", httpField)
	}
}

func oneofNames(oneofs protoreflect.OneofDescriptors) []protoreflect.Name {
	names := make([]protoreflect.Name, oneofs.Len())
	for index := range oneofs.Len() {
		names[index] = oneofs.Get(index).Name()
	}
	return names
}

func TestMetricsPayloadPreservesMapAndHistogramBytes(t *testing.T) {
	t.Parallel()

	wantCodes := map[int32]uint64{200: 90, 503: 10}
	wantHistogram := []byte{0xde, 0xad, 0xbe, 0xef}
	want := &loadtestv1.MetricsDelta{
		Counters:         &loadtestv1.MetricCounters{StatusCodes: wantCodes},
		LatencyHistogram: wantHistogram,
	}
	encoded, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	got := &loadtestv1.MetricsDelta{}
	if err := proto.Unmarshal(encoded, got); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if !reflect.DeepEqual(got.GetCounters().GetStatusCodes(), wantCodes) {
		t.Errorf("status codes = %v, want %v", got.GetCounters().GetStatusCodes(), wantCodes)
	}
	if !reflect.DeepEqual(got.GetLatencyHistogram(), wantHistogram) {
		t.Errorf("histogram bytes = %x, want %x", got.GetLatencyHistogram(), wantHistogram)
	}
}
