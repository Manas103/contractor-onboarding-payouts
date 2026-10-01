// cmd/stripemockserver runs internal/stripemock as a real standalone HTTP
// server on loopback, so the real stripe-go SDK (internal/stripeclient)
// talks to it over a genuine TCP connection. See internal/stripemock for why
// this stands in for a live Stripe test-mode account.
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/Manas103/contractor-onboarding-payouts/internal/stripemock"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8899", "listen address")
	faultRate := flag.Float64("fault-rate", 0.0, "fraction of /v1/transfers calls that fail transiently")
	flag.Parse()

	srv := stripemock.New(*faultRate)
	log.Printf("mock Stripe server listening on %s (fault-rate=%.3f)", *addr, *faultRate)
	if err := http.ListenAndServe(*addr, srv.Handler()); err != nil {
		log.Fatal(err)
	}
}
