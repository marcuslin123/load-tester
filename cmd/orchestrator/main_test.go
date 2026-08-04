package main

import "testing"

func TestParseAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "default", want: ":9090"},
		{name: "override", args: []string{"-addr", "127.0.0.1:19090"}, want: "127.0.0.1:19090"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			address, err := parseAddress(test.args)
			if err != nil {
				t.Fatalf("parseAddress() error = %v", err)
			}
			if address != test.want {
				t.Fatalf("address = %q, want %q", address, test.want)
			}
		})
	}
}

func TestParseAddressRejectsPositionalArguments(t *testing.T) {
	t.Parallel()

	if _, err := parseAddress([]string{"unexpected"}); err == nil {
		t.Fatal("parseAddress() error = nil, want unexpected argument error")
	}
}
