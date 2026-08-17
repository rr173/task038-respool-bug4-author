package respool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type acquireResult struct {
	l   *Lease
	err error
}

func acquireAsync(t *testing.T, p *Pool, ctx context.Context, weight int, ttl time.Duration, pri Priority) <-chan acquireResult {
	t.Helper()
	ch := make(chan acquireResult, 1)
	go func() {
		l, err := p.Acquire(ctx, weight, ttl, pri)
		ch <- acquireResult{l: l, err: err}
	}()
	return ch
}

func expectGranted(t *testing.T, ch <-chan acquireResult, what string) *Lease {
	t.Helper()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("%s: expected grant, got %v", what, r.err)
		}
		return r.l
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: grant timed out", what)
		return nil
	}
}

func expectNotGranted(t *testing.T, ch <-chan acquireResult, what string) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("%s: unexpectedly completed (err=%v)", what, r.err)
	case <-time.After(60 * time.Millisecond):
	}
}

// waitForWaiters polls until the pool reports at least n queued waiters, so a
// test can establish deterministic queue order across async acquirers.
func waitForWaiters(t *testing.T, p *Pool, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if p.Stats().Waiters >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d waiters (got %d)", n, p.Stats().Waiters)
}

func assertConservation(t *testing.T, p *Pool) {
	t.Helper()
	st := p.Stats()
	if st.Available+st.InUse != st.Capacity {
		t.Fatalf("conservation violated: avail(%d)+inUse(%d) != cap(%d)", st.Available, st.InUse, st.Capacity)
	}
}

func TestNewInvalidCapacity(t *testing.T) {
	if _, err := New(0); !errors.Is(err, ErrInvalidCapacity) {
		t.Fatalf("expected ErrInvalidCapacity for 0, got %v", err)
	}
	if _, err := New(-1); !errors.Is(err, ErrInvalidCapacity) {
		t.Fatalf("expected ErrInvalidCapacity for -1, got %v", err)
	}
}

func TestAcquireInvalidInputs(t *testing.T) {
	p, _ := New(10)
	defer p.Close()
	ctx := context.Background()

	if _, err := p.Acquire(ctx, 0, time.Second, PriorityNormal); !errors.Is(err, ErrInvalidWeight) {
		t.Fatalf("weight 0: got %v", err)
	}
	if _, err := p.Acquire(ctx, -1, time.Second, PriorityNormal); !errors.Is(err, ErrInvalidWeight) {
		t.Fatalf("weight -1: got %v", err)
	}
	if _, err := p.Acquire(ctx, 1, 0, PriorityNormal); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("ttl 0: got %v", err)
	}
	if _, err := p.Acquire(ctx, 1, time.Second, Priority(-1)); !errors.Is(err, ErrInvalidPriority) {
		t.Fatalf("priority -1: got %v", err)
	}
	if _, err := p.Acquire(ctx, 1, time.Second, priorityCount); !errors.Is(err, ErrInvalidPriority) {
		t.Fatalf("priority overflow: got %v", err)
	}
}

func TestAcquireReleaseConservation(t *testing.T) {
	p, _ := New(10)
	defer p.Close()

	l, err := p.Acquire(context.Background(), 3, time.Second, PriorityNormal)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if l.Weight != 3 || l.ID == 0 {
		t.Fatalf("bad lease: %+v", l)
	}
	st := p.Stats()
	if st.Available != 7 || st.InUse != 3 || st.ActiveLeases != 1 || st.Waiters != 0 {
		t.Fatalf("stats after acquire: %+v", st)
	}
	assertConservation(t, p)

	if err := p.Release(l.ID); err != nil {
		t.Fatalf("release: %v", err)
	}
	st = p.Stats()
	if st.Available != 10 || st.InUse != 0 || st.ActiveLeases != 0 {
		t.Fatalf("stats after release: %+v", st)
	}
}

func TestWeightCeilingImmediateReject(t *testing.T) {
	p, _ := New(10)
	defer p.Close()

	_, err := p.Acquire(context.Background(), 11, time.Second, PriorityNormal)
	if !errors.Is(err, ErrWeightExceedsCapacity) {
		t.Fatalf("expected ErrWeightExceedsCapacity, got %v", err)
	}
	// Must not have queued.
	if st := p.Stats(); st.Waiters != 0 {
		t.Fatalf("oversized acquire queued: %+v", st)
	}
	assertConservation(t, p)
}

func TestWaiterGrantedOnRelease(t *testing.T) {
	p, _ := New(10)
	defer p.Close()

	hold, _ := p.Acquire(context.Background(), 10, time.Minute, PriorityNormal)
	ch := acquireAsync(t, p, context.Background(), 5, time.Minute, PriorityNormal)
	waitForWaiters(t, p, 1)
	expectNotGranted(t, ch, "waiter before release")
	assertConservation(t, p)

	if err := p.Release(hold.ID); err != nil {
		t.Fatalf("release hold: %v", err)
	}
	l := expectGranted(t, ch, "waiter after release")
	if l.Weight != 5 {
		t.Fatalf("granted wrong weight: %d", l.Weight)
	}
	assertConservation(t, p)
}

func TestWaiterGrantedOnReclaim(t *testing.T) {
	p, _ := New(10)
	defer p.Close()

	_, _ = p.Acquire(context.Background(), 10, 5*time.Millisecond, PriorityNormal)
	ch := acquireAsync(t, p, context.Background(), 4, time.Minute, PriorityNormal)
	waitForWaiters(t, p, 1)
	expectNotGranted(t, ch, "waiter before expiry")
	assertConservation(t, p)

	time.Sleep(20 * time.Millisecond) // let the lease expire
	reclaimed := p.Reclaim()
	if len(reclaimed) != 1 {
		t.Fatalf("expected 1 reclaimed, got %d", len(reclaimed))
	}
	l := expectGranted(t, ch, "waiter after reclaim")
	if l.Weight != 4 {
		t.Fatalf("granted wrong weight: %d", l.Weight)
	}
	assertConservation(t, p)
}

func TestReaperWakesWaiter(t *testing.T) {
	p, _ := New(10, WithReaper(2*time.Millisecond))
	defer p.Close()

	_, _ = p.Acquire(context.Background(), 10, 5*time.Millisecond, PriorityNormal)
	ch := acquireAsync(t, p, context.Background(), 3, time.Minute, PriorityNormal)
	// No explicit Reclaim; the background reaper must wake the waiter.
	l := expectGranted(t, ch, "waiter via reaper")
	if l.Weight != 3 {
		t.Fatalf("granted wrong weight: %d", l.Weight)
	}
	assertConservation(t, p)
}

func TestStrictFIFOHeadOfLineBlocking(t *testing.T) {
	p, _ := New(10, WithSkipPolicy(SkipNone))
	defer p.Close()

	hold, _ := p.Acquire(context.Background(), 8, time.Minute, PriorityNormal) // avail 2
	aCh := acquireAsync(t, p, context.Background(), 5, time.Minute, PriorityNormal)
	waitForWaiters(t, p, 1) // A is queued at the head
	bCh := acquireAsync(t, p, context.Background(), 2, time.Minute, PriorityNormal)
	waitForWaiters(t, p, 2) // B is queued behind A

	// B (weight 2) fits in the 2 available but must NOT jump A (weight 5) under
	// strict FIFO: the head waiter blocks every later one even when it would fit.
	expectNotGranted(t, bCh, "B should not jump strict FIFO head")
	expectNotGranted(t, aCh, "A should not be granted yet")
	assertConservation(t, p)

	// Free everything: avail returns to 10, A (5) then B (2) are granted in order.
	if err := p.Release(hold.ID); err != nil {
		t.Fatalf("release hold: %v", err)
	}
	la := expectGranted(t, aCh, "A after release")
	lb := expectGranted(t, bCh, "B after A")
	if la.ID >= lb.ID {
		t.Fatalf("A should be granted before B: A=%d B=%d", la.ID, lb.ID)
	}
	assertConservation(t, p)
}

func TestBestFitSkip(t *testing.T) {
	// avail 2 after hold. Head A (5) blocks; B (2) fits and may skip A once a
	// tryGrant runs. Releasing one unit from a second hold triggers tryGrant
	// with avail=3: A still needs 5 (blocked), B needs 2 (fits) -> B granted.
	p, _ := New(10, WithSkipPolicy(SkipBestFit))
	defer p.Close()

	h1, _ := p.Acquire(context.Background(), 7, time.Minute, PriorityNormal) // avail 3
	h2, _ := p.Acquire(context.Background(), 3, time.Minute, PriorityNormal) // avail 0
	aCh := acquireAsync(t, p, context.Background(), 5, time.Minute, PriorityNormal) // head, blocks
	waitForWaiters(t, p, 1)
	bCh := acquireAsync(t, p, context.Background(), 2, time.Minute, PriorityNormal) // fits when room appears
	waitForWaiters(t, p, 2)

	// Release h2 -> avail 3. Head A(5) still blocks (5>3); B(2) fits (2<=3) and
	// is granted under best-fit skip.
	if err := p.Release(h2.ID); err != nil {
		t.Fatalf("release h2: %v", err)
	}
	lb := expectGranted(t, bCh, "B best-fit skip")
	if lb.Weight != 2 {
		t.Fatalf("granted wrong weight: %d", lb.Weight)
	}
	expectNotGranted(t, aCh, "A still blocked")
	_ = h1
	assertConservation(t, p)
}

func TestPriorityOrdering(t *testing.T) {
	p, _ := New(10)
	defer p.Close()

	hold, _ := p.Acquire(context.Background(), 10, time.Minute, PriorityNormal)
	// Queue low, then normal, then high. Grant order must be high -> normal -> low.
	lowCh := acquireAsync(t, p, context.Background(), 2, time.Minute, PriorityLow)
	waitForWaiters(t, p, 1)
	normCh := acquireAsync(t, p, context.Background(), 2, time.Minute, PriorityNormal)
	waitForWaiters(t, p, 2)
	highCh := acquireAsync(t, p, context.Background(), 2, time.Minute, PriorityHigh)
	waitForWaiters(t, p, 3)

	if err := p.Release(hold.ID); err != nil {
		t.Fatalf("release: %v", err)
	}
	lh := expectGranted(t, highCh, "high first")
	ln := expectGranted(t, normCh, "normal second")
	ll := expectGranted(t, lowCh, "low last")
	if !(lh.ID < ln.ID && ln.ID < ll.ID) {
		t.Fatalf("priority order wrong: high=%d normal=%d low=%d", lh.ID, ln.ID, ll.ID)
	}
	assertConservation(t, p)
}

func TestReleaseDistinguishableStates(t *testing.T) {
	p, _ := New(10)
	defer p.Close()

	// Double release -> ErrLeaseAlreadyReleased.
	l, _ := p.Acquire(context.Background(), 2, time.Minute, PriorityNormal)
	if err := p.Release(l.ID); err != nil {
		t.Fatalf("first release: %v", err)
	}
	if err := p.Release(l.ID); !errors.Is(err, ErrLeaseAlreadyReleased) {
		t.Fatalf("double release: got %v", err)
	}

	// Expired-but-not-reaped -> ErrLeaseExpired on release.
	exp, _ := p.Acquire(context.Background(), 2, 5*time.Millisecond, PriorityNormal)
	time.Sleep(20 * time.Millisecond)
	if err := p.Release(exp.ID); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expired-not-reaped release: got %v", err)
	}

	// Reaped -> ErrLeaseExpired (lease tombstoned, distinguishable from not-found).
	rep, _ := p.Acquire(context.Background(), 2, 5*time.Millisecond, PriorityNormal)
	time.Sleep(20 * time.Millisecond)
	p.Reclaim()
	if err := p.Release(rep.ID); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("reaped release: got %v", err)
	}

	// Unknown id -> ErrLeaseNotFound.
	if err := p.Release(99999); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("unknown release: got %v", err)
	}
	assertConservation(t, p)
}

func TestRenew(t *testing.T) {
	p, _ := New(10)
	defer p.Close()

	l, _ := p.Acquire(context.Background(), 2, 50*time.Millisecond, PriorityNormal)
	orig := l.Deadline
	dl, err := p.Renew(l.ID, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if !dl.After(orig) {
		t.Fatalf("deadline not extended: orig=%v new=%v", orig, dl)
	}

	// Renew with non-positive extension fails.
	if _, err := p.Renew(l.ID, 0); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("renew 0: got %v", err)
	}

	// Renew after expiry fails.
	time.Sleep(200 * time.Millisecond)
	if _, err := p.Renew(l.ID, 50*time.Millisecond); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("renew expired: got %v", err)
	}

	// Renew released fails.
	l2, _ := p.Acquire(context.Background(), 1, time.Minute, PriorityNormal)
	p.Release(l2.ID)
	if _, err := p.Renew(l2.ID, 50*time.Millisecond); !errors.Is(err, ErrLeaseAlreadyReleased) {
		t.Fatalf("renew released: got %v", err)
	}

	// Renew unknown fails.
	if _, err := p.Renew(99999, 50*time.Millisecond); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("renew unknown: got %v", err)
	}
}

func TestResizeGrow(t *testing.T) {
	p, _ := New(5)
	defer p.Close()

	_, _ = p.Acquire(context.Background(), 5, time.Minute, PriorityNormal) // full
	ch := acquireAsync(t, p, context.Background(), 3, time.Minute, PriorityNormal)
	waitForWaiters(t, p, 1)
	expectNotGranted(t, ch, "before grow")

	if err := p.Resize(10); err != nil {
		t.Fatalf("resize grow: %v", err)
	}
	l := expectGranted(t, ch, "after grow")
	if l.Weight != 3 {
		t.Fatalf("granted wrong weight: %d", l.Weight)
	}
	assertConservation(t, p)
}

func TestResizeShrinkDeficitReserved(t *testing.T) {
	p, _ := New(10)
	defer p.Close()

	hold, _ := p.Acquire(context.Background(), 8, time.Minute, PriorityNormal) // avail 2, inUse 8
	if err := p.Resize(5); err != nil {
		t.Fatalf("resize shrink: %v", err)
	}
	st := p.Stats()
	if st.Capacity != 5 {
		t.Fatalf("cap=%d", st.Capacity)
	}
	// inUse unchanged (8), available goes negative (reserved deficit).
	if st.InUse != 8 || st.Available != -3 {
		t.Fatalf("after shrink stats=%+v", st)
	}
	assertConservation(t, p) // -3 + 8 == 5

	// New acquire must block while deficit reserved.
	ch := acquireAsync(t, p, context.Background(), 1, time.Minute, PriorityNormal)
	waitForWaiters(t, p, 1)
	expectNotGranted(t, ch, "acquire during deficit")

	// Releasing the 8-unit lease restores available to 5 and grants the waiter.
	if err := p.Release(hold.ID); err != nil {
		t.Fatalf("release: %v", err)
	}
	l := expectGranted(t, ch, "waiter after deficit cleared")
	if l.Weight != 1 {
		t.Fatalf("granted wrong weight: %d", l.Weight)
	}
	assertConservation(t, p)
}

func TestResizeShrinkCancelsOversizedWaiter(t *testing.T) {
	p, _ := New(10)
	defer p.Close()

	_, _ = p.Acquire(context.Background(), 10, time.Minute, PriorityNormal) // full
	ch := acquireAsync(t, p, context.Background(), 8, time.Minute, PriorityNormal)
	waitForWaiters(t, p, 1)

	if err := p.Resize(5); err != nil {
		t.Fatalf("resize: %v", err)
	}
	select {
	case r := <-ch:
		if !errors.Is(r.err, ErrWeightExceedsCapacity) {
			t.Fatalf("expected oversized waiter canceled, got %v", r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("oversized waiter not canceled")
	}
	// Canceled waiter holds no units.
	assertConservation(t, p)
	if st := p.Stats(); st.Waiters != 0 {
		t.Fatalf("waiters after cancel: %+v", st)
	}
}

func TestContextCancelRemovesWaiter(t *testing.T) {
	p, _ := New(10)
	defer p.Close()

	_, _ = p.Acquire(context.Background(), 10, time.Minute, PriorityNormal) // full
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	ch := acquireAsync(t, p, ctx, 3, time.Minute, PriorityNormal)
	select {
	case r := <-ch:
		if !errors.Is(r.err, context.DeadlineExceeded) {
			t.Fatalf("expected DeadlineExceeded, got %v", r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("canceled waiter did not return")
	}
	// Must be removed from the queue.
	if st := p.Stats(); st.Waiters != 0 {
		t.Fatalf("waiter not removed: %+v", st)
	}
	assertConservation(t, p)
}

func TestCloseCancelsWaiters(t *testing.T) {
	p, _ := New(10)
	defer p.Close()

	_, _ = p.Acquire(context.Background(), 10, time.Minute, PriorityNormal) // full
	ch := acquireAsync(t, p, context.Background(), 3, time.Minute, PriorityNormal)
	waitForWaiters(t, p, 1)

	p.Close()
	select {
	case r := <-ch:
		if !errors.Is(r.err, ErrPoolClosed) {
			t.Fatalf("expected ErrPoolClosed, got %v", r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("waiter not canceled on close")
	}

	// New acquires after close are rejected.
	if _, err := p.Acquire(context.Background(), 1, time.Second, PriorityNormal); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("acquire after close: got %v", err)
	}
}

func TestConcurrentAcquireRelease(t *testing.T) {
	// Long TTL to isolate accounting from expiry interference.
	p, _ := New(20)
	defer p.Close()

	const goroutines = 12
	const iterations = 40
	var wg sync.WaitGroup
	wg.Add(goroutines)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				l, err := p.Acquire(context.Background(), 3, time.Minute, PriorityNormal)
				if err != nil {
					return
				}
				time.Sleep(time.Millisecond)
				p.Release(l.ID)
			}
		}()
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		st := p.Stats()
		t.Fatalf("concurrent test hung: stats=%+v", st)
	}
	assertConservation(t, p)
	st := p.Stats()
	if st.InUse != 0 || st.Waiters != 0 {
		t.Fatalf("final stats not drained: %+v", st)
	}
}
