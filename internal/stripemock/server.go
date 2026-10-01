// Package stripemock implements the subset of Stripe's test-mode transfer
// API this project exercises: idempotency-key enforcement, a transfer
// (payout) object lifecycle, and configurable transient-failure injection.
//
// There is no live Stripe account available in this headless environment
// (see README "honest framing"). This server lets the real stripe-go SDK
// make real HTTP calls against real idempotency and retry logic; only the
// far end (an actual Stripe data center) is substituted.
package stripemock

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type cachedResponse struct {
	status int
	body   []byte
}

type ledgerEntry struct {
	ID            string `json:"id"`
	ContractorID  string `json:"contractor_id"`
	BatchPeriod   string `json:"batch_period"`
	AmountCents   int64  `json:"amount_cents"`
	Created       int64  `json:"created"`
}

// Server is a real net/http server, not a fake in-process RoundTripper: the
// stripe-go SDK talks to it over a real TCP loopback connection exactly as
// it would talk to api.stripe.com.
type Server struct {
	mu         sync.Mutex
	idemCache  map[string]cachedResponse
	ledger     map[string]ledgerEntry // transfer id -> entry, success only
	faultRate  float64
	rng        *rand.Rand
	injected   int64 // atomic: count of requests that were made to fail on purpose
	totalCalls int64 // atomic: every inbound /v1/transfers call, cached or not
	nextID     int64
}

func New(faultRate float64) *Server {
	return &Server{
		idemCache: make(map[string]cachedResponse),
		ledger:    make(map[string]ledgerEntry),
		faultRate: faultRate,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/transfers", s.handleTransfer)
	mux.HandleFunc("/v1/mock/stats", s.handleStats)
	mux.HandleFunc("/v1/mock/ledger", s.handleLedger)
	mux.HandleFunc("/v1/mock/reset_faultrate", s.handleResetFaultRate)
	return mux
}

func (s *Server) handleTransfer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	atomic.AddInt64(&s.totalCalls, 1)

	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		http.Error(w, `{"error":{"message":"missing Idempotency-Key"}}`, http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	if cached, ok := s.idemCache[key]; ok {
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Mock-Idempotent-Replay", "true")
		w.WriteHeader(cached.status)
		w.Write(cached.body)
		return
	}
	s.mu.Unlock()

	if err := r.ParseForm(); err != nil {
		http.Error(w, `{"error":{"message":"bad form body"}}`, http.StatusBadRequest)
		return
	}

	// Fault injection applies only to fresh attempts. A key that has not yet
	// succeeded is not cached, so the client's retry wrapper will hit this
	// branch again on its next attempt with the same key.
	if s.faultRate > 0 {
		s.mu.Lock()
		roll := s.rng.Float64()
		s.mu.Unlock()
		if roll < s.faultRate {
			atomic.AddInt64(&s.injected, 1)
			code := http.StatusServiceUnavailable
			if int(atomic.LoadInt64(&s.injected))%2 == 0 {
				code = http.StatusInternalServerError
			}
			http.Error(w, fmt.Sprintf(`{"error":{"message":"mock transient provider failure","type":"api_error"}}`), code)
			return
		}
	}

	amount, _ := strconv.ParseInt(r.PostForm.Get("amount"), 10, 64)
	contractorID := r.PostForm.Get("metadata[contractor_id]")
	batchPeriod := r.PostForm.Get("metadata[batch_period]")

	s.mu.Lock()
	s.nextID++
	id := fmt.Sprintf("tr_mock_%08d", s.nextID)
	entry := ledgerEntry{
		ID:           id,
		ContractorID: contractorID,
		BatchPeriod:  batchPeriod,
		AmountCents:  amount,
		Created:      time.Now().Unix(),
	}
	s.ledger[id] = entry
	body, _ := json.Marshal(map[string]any{
		"id":       id,
		"object":   "transfer",
		"amount":   amount,
		"currency": "usd",
		"created":  entry.Created,
		"metadata": map[string]string{"contractor_id": contractorID, "batch_period": batchPeriod},
	})
	s.idemCache[key] = cachedResponse{status: http.StatusOK, body: body}
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	ledgerSize := len(s.ledger)
	s.mu.Unlock()
	json.NewEncoder(w).Encode(map[string]any{
		"injected_failures": atomic.LoadInt64(&s.injected),
		"total_calls":       atomic.LoadInt64(&s.totalCalls),
		"successful_transfers": ledgerSize,
		"fault_rate":        s.faultRate,
	})
}

func (s *Server) handleLedger(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	out := make([]ledgerEntry, 0, len(s.ledger))
	for _, e := range s.ledger {
		out = append(out, e)
	}
	s.mu.Unlock()
	json.NewEncoder(w).Encode(out)
}

// handleResetFaultRate lets the fault-injection benchmark (claim 5) and the
// kill-test benchmark (claim 6) share one long-lived mock server process
// while using different fault rates for their respective runs.
func (s *Server) handleResetFaultRate(w http.ResponseWriter, r *http.Request) {
	rate, err := strconv.ParseFloat(r.URL.Query().Get("rate"), 64)
	if err != nil {
		http.Error(w, "bad rate", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.faultRate = rate
	s.mu.Unlock()
	atomic.StoreInt64(&s.injected, 0)
	atomic.StoreInt64(&s.totalCalls, 0)
	log.Printf("fault rate reset to %v", rate)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) InjectedFailures() int64 { return atomic.LoadInt64(&s.injected) }
