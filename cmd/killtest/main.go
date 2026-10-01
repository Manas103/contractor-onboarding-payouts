// cmd/killtest measures claim 6 and, by its final state, claim 7: it spawns
// the real cmd/payout binary as a subprocess (holding its own PID, never a
// PID discovered from ps/tasklist), lets it run for a short random delay,
// and sends it a real SIGKILL, exactly like a crashed process. It repeats
// this --kills times (200 by default) against one batch, then lets a final
// run drain whatever is left, so the batch actually finishes. The
// correctness claim is checked afterwards by cmd/reconcile: 0 duplicate
// payouts despite the repeated mid-batch kills.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os/exec"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func pendingCount(ctx context.Context, db *sql.DB, batch int) int64 {
	var n int64
	db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM payout_queue q
		LEFT JOIN payouts p ON p.contractor_id = q.contractor_id AND p.batch_period = q.batch_period
		WHERE q.batch_period = $1 AND p.id IS NULL`, batch).Scan(&n)
	return n
}

func main() {
	batch := flag.Int("batch", 1, "batch period to process")
	dsn := flag.String("dsn", "postgres://contractor_app:contractor_app_pw@127.0.0.1:5432/contractor_payouts?sslmode=disable", "Postgres DSN")
	stripeURL := flag.String("stripe-url", "http://127.0.0.1:8899", "mock Stripe base URL")
	payoutBin := flag.String("payout-bin", "./bin/payout", "path to the built cmd/payout binary")
	kills := flag.Int("kills", 200, "number of mid-batch SIGKILLs to perform")
	slowMs := flag.Int("slow-ms", 3, "per-record delay inside the payout worker, so kills reliably land mid-batch")
	flag.Parse()

	db, err := sql.Open("pgx", *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	actualKills := 0

	for i := 0; i < *kills; i++ {
		before := pendingCount(ctx, db, *batch)
		if before == 0 {
			fmt.Printf("batch drained after %d kills (no pending work left to interrupt)\n", actualKills)
			break
		}
		cmd := exec.Command(*payoutBin, "--batch", fmt.Sprint(*batch), "--dsn", *dsn,
			"--stripe-url", *stripeURL, "--slow-ms", fmt.Sprint(*slowMs))
		if err := cmd.Start(); err != nil {
			log.Fatalf("start payout worker: %v", err)
		}
		pid := cmd.Process.Pid

		delay := time.Duration(20+rng.Intn(80)) * time.Millisecond
		time.Sleep(delay)

		if cmd.ProcessState == nil {
			if err := cmd.Process.Kill(); err != nil {
				log.Printf("kill pid %d: %v (process may have already exited)", pid, err)
			} else {
				actualKills++
			}
		}
		cmd.Wait() // reap, ignore the (expected) killed-process error

		if (i+1)%25 == 0 {
			fmt.Printf("  ... %d/%d kill cycles done, %d pending\n", i+1, *kills, pendingCount(ctx, db, *batch))
		}
	}

	fmt.Printf("performed %d real SIGKILLs against the payout worker (pid held per-spawn, never discovered from ps/tasklist)\n", actualKills)

	remaining := pendingCount(ctx, db, *batch)
	fmt.Printf("draining final run, %d contractors still pending...\n", remaining)
	final := exec.Command(*payoutBin, "--batch", fmt.Sprint(*batch), "--dsn", *dsn, "--stripe-url", *stripeURL)
	out, err := final.CombinedOutput()
	fmt.Println(string(out))
	if err != nil {
		log.Fatalf("final drain run failed: %v", err)
	}
	fmt.Printf("batch %d fully drained after %d SIGKILLs\n", *batch, actualKills)
}
