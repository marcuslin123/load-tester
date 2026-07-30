package main

import (
	"errors"
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/marcuslin123/load-tester/internal/targetapp"
)

func main() {
	address := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	server := newServer(*address)
	log.Printf("target app listening on %s", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func newServer(address string) *http.Server {
	return &http.Server{
		Addr:              address,
		Handler:           targetapp.NewHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
}
