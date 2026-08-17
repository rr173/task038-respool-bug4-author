// Command go-task-check runs the resource pool smoke test. With --smoke-test it
// exercises a representative slice of the pool's behavior end to end and exits.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"go-task-check/respool"
)

// waitForWaiters polls the pool until it reports at least n queued waiters, so
// the smoke test can establish deterministic queue order across goroutines.
func waitForWaiters(p *respool.Pool, n int) {
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if p.Stats().Waiters >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func main() {
	smoke := flag.Bool("smoke-test", false, "run the built-in smoke test and exit")
	flag.Parse()
	if !*smoke {
		fmt.Println("respool runner: pass --smoke-test to run the built-in checks")
		return
	}
	if err := runSmoke(); err != nil {
		fmt.Fprintf(os.Stderr, "SMOKE FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("SMOKE OK")
}

func runSmoke() error {
	// 1. Basic acquire/release and capacity conservation.
	p, err := respool.New(10)
	if err != nil {
		return fmt.Errorf("new: %w", err)
	}
	defer p.Close()
	l1, err := p.Acquire(context.Background(), 3, time.Second, respool.PriorityNormal)
	if err != nil {
		return fmt.Errorf("acquire 3: %w", err)
	}
	st := p.Stats()
	if st.Available != 7 || st.InUse != 3 || st.ActiveLeases != 1 || st.Waiters != 0 {
		return fmt.Errorf("stats after acquire: %+v", st)
	}
	if err := p.Release(l1.ID); err != nil {
		return fmt.Errorf("release: %w", err)
	}
	st = p.Stats()
	if st.Available != 10 || st.InUse != 0 || st.ActiveLeases != 0 {
		return fmt.Errorf("stats after release: %+v", st)
	}

	// 2. Weight above capacity is rejected immediately and never queued.
	if _, err := p.Acquire(context.Background(), 11, time.Second, respool.PriorityNormal); !errors.Is(err, respool.ErrWeightExceedsCapacity) {
		return fmt.Errorf("expected ErrWeightExceedsCapacity, got %v", err)
	}
	if s := p.Stats(); s.Waiters != 0 {
		return fmt.Errorf("oversized acquire queued: %+v", s)
	}

	// 3. TTL expiry + reclaim wakes a blocked waiter.
	p2, _ := respool.New(10)
	defer p2.Close()
	full, _ := p2.Acquire(context.Background(), 10, 5*time.Millisecond, respool.PriorityNormal)
	_ = full
	granted := make(chan *respool.Lease, 1)
	go func() {
		l, e := p2.Acquire(context.Background(), 5, time.Second, respool.PriorityNormal)
		if e != nil {
			granted <- nil
			return
		}
		granted <- l
	}()
	time.Sleep(20 * time.Millisecond) // let the full lease expire
	p2.Reclaim()
	select {
	case gl := <-granted:
		if gl == nil {
			return fmt.Errorf("waiter not granted after reclaim")
		}
	case <-time.After(time.Second):
		return fmt.Errorf("waiter grant timed out")
	}

	// 4. Strict FIFO: the head waiter blocks a smaller later waiter.
	p3, _ := respool.New(10, respool.WithSkipPolicy(respool.SkipNone))
	defer p3.Close()
	hold, _ := p3.Acquire(context.Background(), 8, time.Second, respool.PriorityNormal) // available 2
	aDone := make(chan *respool.Lease, 1)
	bDone := make(chan *respool.Lease, 1)
	go func() { l, _ := p3.Acquire(context.Background(), 5, time.Second, respool.PriorityNormal); aDone <- l }()
	waitForWaiters(p3, 1) // A is queued at the head before B starts
	go func() { l, _ := p3.Acquire(context.Background(), 2, time.Second, respool.PriorityNormal); bDone <- l }()
	waitForWaiters(p3, 2) // both queued
	// B (2) fits the 2 available but must not jump the head A (5) under strict FIFO.
	select {
	case <-bDone:
		return fmt.Errorf("B should not jump a strict-FIFO head waiter")
	default:
	}
	p3.Release(hold.ID)
	select {
	case <-aDone:
	case <-time.After(time.Second):
		return fmt.Errorf("A not granted after release")
	}
	select {
	case <-bDone:
	case <-time.After(time.Second):
		return fmt.Errorf("B not granted after A")
	}

	// 5. Shrink cancels a waiter whose weight exceeds the new capacity.
	p4, _ := respool.New(10)
	defer p4.Close()
	_, _ = p4.Acquire(context.Background(), 10, time.Second, respool.PriorityNormal) // full
	wErr := make(chan error, 1)
	go func() { _, e := p4.Acquire(context.Background(), 8, time.Second, respool.PriorityNormal); wErr <- e }()
	time.Sleep(30 * time.Millisecond)
	if err := p4.Resize(5); err != nil {
		return fmt.Errorf("resize: %w", err)
	}
	select {
	case e := <-wErr:
		if !errors.Is(e, respool.ErrWeightExceedsCapacity) {
			return fmt.Errorf("expected oversized waiter canceled, got %v", e)
		}
	case <-time.After(time.Second):
		return fmt.Errorf("oversized waiter not canceled after shrink")
	}
	return nil
}
