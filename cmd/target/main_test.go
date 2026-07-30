package main

import (
	"testing"
	"time"
)

func TestNewServerConfiguresTargetApp(t *testing.T) {
	server := newServer(":9090")

	if server.Addr != ":9090" {
		t.Fatalf("address = %q, want %q", server.Addr, ":9090")
	}
	if server.Handler == nil {
		t.Fatal("handler is nil")
	}
	if server.ReadHeaderTimeout != 5*time.Second {
		t.Fatalf("read header timeout = %s, want 5s", server.ReadHeaderTimeout)
	}
}
