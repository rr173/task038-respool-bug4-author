// Package respool implements a weighted resource pool that grants
// time-limited leases over a finite integer capacity.
//
// Callers acquire units by weight; each successful acquire returns a lease
// with a deadline. Leases may be released early, renewed before expiry, or
// reclaimed automatically once their deadline passes. When capacity cannot
// satisfy an acquire immediately, the request queues as a waiter and is
// granted (in priority order, FIFO within a priority) once units free up.
package respool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Priority ranks waiters: higher values are granted first. Within a single
// priority level waiters are served in arrival (FIFO) order.
type Priority int

const (
	// PriorityLow is the lowest priority.
	PriorityLow Priority = iota
	// PriorityNormal is the default priority.
	PriorityNormal
	// PriorityHigh is the highest priority.
	PriorityHigh

	// priorityCount bounds valid priority values for validation.
	priorityCount
)

// SkipPolicy controls whether a smaller waiter may jump a head-of-line
// waiter that cannot currently be satisfied.
type SkipPolicy int

const (
	// SkipNone is the default: strict FIFO. The head waiter blocks every
	// later waiter until it is satisfied, preventing starvation.
	SkipNone SkipPolicy = iota
	// SkipBestFit allows a later, smaller waiter to be granted while the
	// head waiter remains blocked because the pool cannot fit it.
	SkipBestFit
)

// Lease state.
type leaseState int

const (
	stateActive leaseState = iota
	stateReleased // explicitly released by the caller
	stateExpired  // deadline passed and reclaimed
)

// Lease represents a granted holding of weight units until Deadline.
type Lease struct {
	ID        uint64
	Weight    int
	Priority  Priority
	AcquiredAt time.Time
	Deadline  time.Time

	state leaseState
}

// Stats is a point-in-time snapshot of pool occupancy.
type Stats struct {
	Capacity     int
	Available    int
	InUse        int
	Waiters      int
	ActiveLeases int
}

// Pool manages allocation of a finite, weighted capacity through leases.
type Pool struct {
	mu sync.Mutex

	capacity  int
	available int
	inUse     int

	leases  map[uint64]*Lease
	nextID  uint64
	nextSeq uint64

	waiters    []*waiter
	skipPolicy SkipPolicy

	closed bool
	done   chan struct{}
	wg     sync.WaitGroup
}

type waiter struct {
	weight  int
	ttl     time.Duration
	pri     Priority
	seq     uint64
	lease   *Lease
	err     error
	granted bool
	done    chan struct{}
}

// Option configures a Pool at construction.
type Option func(*Pool)

// WithSkipPolicy sets the head-of-line skip policy. Default is SkipNone.
func WithSkipPolicy(p SkipPolicy) Option {
	return func(pl *Pool) { pl.skipPolicy = p }
}

// WithReaper starts a background goroutine that reclaims expired leases every
// interval and wakes any waiters that become satisfiable. If not set, expired
// leases are only reclaimed lazily (on touch) or via an explicit Reclaim call.
func WithReaper(interval time.Duration) Option {
	return func(pl *Pool) {
		if interval <= 0 {
			return
		}
		pl.wg.Add(1)
		go pl.reaper(interval)
	}
}

// New creates a pool with the given total capacity.
func New(capacity int, opts ...Option) (*Pool, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("%w: capacity %d must be positive", ErrInvalidCapacity, capacity)
	}
	p := &Pool{
		capacity:  capacity,
		available: capacity,
		leases:    make(map[uint64]*Lease),
		done:      make(chan struct{}),
	}
	for _, o := range opts {
		o(p)
	}
	return p, nil
}

// Acquire requests weight units for ttl at priority pri.
//
// If weight exceeds the pool capacity it is rejected immediately rather than
// queued, since it could never be satisfied. Otherwise, when the units are
// available and no one is queued ahead, a lease is granted at once. When not,
// the call blocks until the request is satisfied, ctx is canceled, or the pool
// is closed.
func (p *Pool) Acquire(ctx context.Context, weight int, ttl time.Duration, pri Priority) (*Lease, error) {
	if weight <= 0 {
		return nil, fmt.Errorf("%w: weight %d must be positive", ErrInvalidWeight, weight)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("%w: ttl %s must be positive", ErrInvalidTTL, ttl)
	}
	if pri < PriorityLow || pri >= priorityCount {
		return nil, fmt.Errorf("%w: priority %d out of range", ErrInvalidPriority, pri)
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	// Weight above total capacity can never be satisfied: reject, never queue.
	if weight > p.capacity {
		p.mu.Unlock()
		return nil, fmt.Errorf("%w: weight %d > capacity %d", ErrWeightExceedsCapacity, weight, p.capacity)
	}
	p.reclaimExpiredLocked()

	// Fast path: nothing queued ahead and units fit.
	if len(p.waiters) == 0 && p.available >= weight {
		lease := p.grantLocked(weight, ttl, pri)
		p.mu.Unlock()
		return lease, nil
	}

	// Queue as a waiter. A request never jumps ahead of queued waiters.
	w := &waiter{weight: weight, ttl: ttl, pri: pri, done: make(chan struct{})}
	p.insertWaiterLocked(w)
	p.mu.Unlock()

	select {
	case <-w.done:
		if w.err != nil {
			return nil, w.err
		}
		return w.lease, nil
	case <-ctx.Done():
		p.mu.Lock()
		if w.granted {
			// Lost the race: the waiter was granted just as ctx fired.
			p.mu.Unlock()
			return w.lease, nil
		}
		p.removeWaiterLocked(w)
		p.mu.Unlock()
		return nil, ctx.Err()
	}
}

// Release returns a lease's units to the pool early and wakes waiters that can
// now be satisfied.
//
// The returned error distinguishes outcomes: nil for a clean release,
// ErrLeaseAlreadyReleased for a double release, ErrLeaseExpired when the
// lease's deadline has already passed (whether or not the reaper has swept
// it), and ErrLeaseNotFound for an unknown identifier.
func (p *Pool) Release(id uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	l, ok := p.leases[id]
	if !ok {
		return ErrLeaseNotFound
	}
	switch l.state {
	case stateReleased:
		return ErrLeaseAlreadyReleased
	case stateExpired:
		return ErrLeaseExpired
	}
	// Active lease. If its deadline has passed but the reaper has not yet
	// swept it, treat the release as an expiry.
	if time.Now().After(l.Deadline) {
		p.expireLocked(l)
		return ErrLeaseExpired
	}
	p.releaseLocked(l)
	return nil
}

// Renew extends a lease's deadline by extension, but only while it is still
// active and has not yet expired. Renewing an expired, released, or unknown
// lease fails with the corresponding error.
func (p *Pool) Renew(id uint64, extension time.Duration) (time.Time, error) {
	if extension <= 0 {
		return time.Time{}, fmt.Errorf("%w: extension %s must be positive", ErrInvalidTTL, extension)
	}
	l, ok := p.leases[id]
	if !ok {
		return time.Time{}, ErrLeaseNotFound
	}
	switch l.state {
	case stateReleased:
		return time.Time{}, ErrLeaseAlreadyReleased
	case stateExpired:
		return time.Time{}, ErrLeaseExpired
	}
	now := time.Now()
	if now.After(l.Deadline) {
		// Expired but not yet swept: mark and reject the renewal.
		p.expireLocked(l)
		return time.Time{}, ErrLeaseExpired
	}
	base := l.Deadline
	if now.After(base) {
		base = now
	}
	l.Deadline = base.Add(extension)
	return l.Deadline, nil
}

// Reclaim sweeps all active leases whose deadline has passed, reclaims their
// units, and grants any waiters that become satisfiable. It returns the IDs of
// the leases reclaimed. Safe to call concurrently; idempotent.
func (p *Pool) Reclaim() []uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reclaimExpiredLocked()
}

// Resize changes the total capacity.
//
// Growing immediately adds units and may satisfy waiters. Shrinking never
// forcibly revokes active leases: if the new capacity is below current in-use,
// the deficit is reserved and new acquires block until releases restore
// available capacity. Any queued waiter whose weight exceeds the new capacity
// is canceled immediately, since it can no longer be satisfied.
func (p *Pool) Resize(newCapacity int) error {
	if newCapacity <= 0 {
		return fmt.Errorf("%w: capacity %d must be positive", ErrInvalidCapacity, newCapacity)
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if newCapacity == p.capacity {
		return nil
	}
	if newCapacity > p.capacity {
		delta := newCapacity - p.capacity
		p.capacity = newCapacity
		p.available += delta
		p.tryGrantWaitersLocked()
		return nil
	}

	// Shrink. Do not revoke in-use leases; the deficit is reserved.
	delta := p.capacity - newCapacity
	p.capacity = newCapacity
	p.available -= delta
	// Cancel waiters that can no longer be satisfied.
	p.cancelOversizedWaitersLocked(newCapacity)
	p.tryGrantWaitersLocked()
	return nil
}

// Stats returns a snapshot of the pool's current occupancy.
func (p *Pool) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	active := 0
	for _, l := range p.leases {
		if l.state == stateActive {
			active++
		}
	}
	return Stats{
		Capacity:     p.capacity,
		Available:    p.available,
		InUse:        p.inUse,
		Waiters:      len(p.waiters),
		ActiveLeases: active,
	}
}

// Close shuts the pool down: the reaper is stopped and all queued waiters are
// canceled. Active leases remain tracked but no new acquires are accepted.
func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.done)
	// Cancel every still-queued waiter.
	for _, w := range p.waiters {
		w.err = ErrPoolClosed
		close(w.done)
	}
	p.waiters = nil
	p.mu.Unlock()
	p.wg.Wait()
}

// grantLocked allocates a lease for weight/ttl/pri. Caller holds p.mu and has
// already verified the units are available.
func (p *Pool) grantLocked(weight int, ttl time.Duration, pri Priority) *Lease {
	now := time.Now()
	p.nextID++
	l := &Lease{
		ID:         p.nextID,
		Weight:     weight,
		Priority:   pri,
		AcquiredAt: now,
		Deadline:   now.Add(ttl),
		state:      stateActive,
	}
	p.leases[l.ID] = l
	p.available -= weight
	p.inUse += weight
	return l
}

// releaseLocked returns an active lease's units and marks it released.
func (p *Pool) releaseLocked(l *Lease) {
	l.state = stateReleased
	p.available += l.Weight
	p.inUse -= l.Weight
	p.tryGrantWaitersLocked()
}

// expireLocked reclaims an expired lease's units and marks it expired.
func (p *Pool) expireLocked(l *Lease) {
	l.state = stateExpired
	p.available += l.Weight
	p.inUse -= l.Weight
	p.tryGrantWaitersLocked()
}

// reclaimExpiredLocked sweeps active leases past their deadline.
func (p *Pool) reclaimExpiredLocked() []uint64 {
	var reclaimed []uint64
	now := time.Now()
	for _, l := range p.leases {
		if l.state != stateActive {
			continue
		}
		if now.After(l.Deadline) {
			p.expireLocked(l)
			reclaimed = append(reclaimed, l.ID)
		}
	}
	return reclaimed
}

// tryGrantWaitersLocked grants as many queued waiters as currently possible
// under the configured skip policy. All unit accounting is done by
// grantLocked; this function must not touch available/inUse directly.
func (p *Pool) tryGrantWaitersLocked() {
	for len(p.waiters) > 0 {
		head := p.waiters[0]
		if p.available >= head.weight {
			head.lease = p.grantLocked(head.weight, head.ttl, head.pri)
			head.granted = true
			p.waiters = p.waiters[1:]
			close(head.done)
			continue
		}
		if p.skipPolicy == SkipBestFit {
			idx := -1
			for i := 1; i < len(p.waiters); i++ {
				if p.available >= p.waiters[i].weight {
					idx = i
					break
				}
			}
			if idx == -1 {
				return
			}
			w := p.waiters[idx]
			p.waiters = append(p.waiters[:idx], p.waiters[idx+1:]...)
			w.lease = p.grantLocked(w.weight, w.ttl, w.pri)
			w.granted = true
			close(w.done)
			continue
		}
		// Strict FIFO: the head blocks every later waiter.
		return
	}
}

// insertWaiterLocked inserts w in priority order (higher first), preserving
// arrival order within a priority level.
func (p *Pool) insertWaiterLocked(w *waiter) {
	w.seq = p.nextSeq
	p.nextSeq++
	i := 0
	for i < len(p.waiters) {
		if p.waiters[i].pri < w.pri {
			break
		}
		i++
	}
	p.waiters = append(p.waiters, nil)
	copy(p.waiters[i+1:], p.waiters[i:])
	p.waiters[i] = w
}

// removeWaiterLocked removes w from the queue if still present.
func (p *Pool) removeWaiterLocked(w *waiter) {
	for i, x := range p.waiters {
		if x == w {
			p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
			return
		}
	}
}

// cancelOversizedWaitersLocked cancels waiters whose weight exceeds cap, since
// they can no longer be satisfied after a shrink.
func (p *Pool) cancelOversizedWaitersLocked(cap int) {
	kept := make([]*waiter, 0, len(p.waiters))
	for _, w := range p.waiters {
		if w.weight > cap {
			w.err = fmt.Errorf("%w: weight %d > capacity %d after resize", ErrWeightExceedsCapacity, w.weight, cap)
			close(w.done)
			continue
		}
		kept = append(kept, w)
	}
	p.waiters = kept
}

// reaper periodically reclaims expired leases until the pool closes.
func (p *Pool) reaper(interval time.Duration) {
	defer p.wg.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-t.C:
			p.Reclaim()
		}
	}
}

// Errors returned by the pool. Callers may use errors.Is to distinguish them.
var (
	ErrInvalidWeight         = errors.New("respool: invalid weight")
	ErrInvalidTTL            = errors.New("respool: invalid ttl")
	ErrInvalidPriority       = errors.New("respool: invalid priority")
	ErrInvalidCapacity       = errors.New("respool: invalid capacity")
	ErrWeightExceedsCapacity = errors.New("respool: weight exceeds capacity")
	ErrLeaseNotFound         = errors.New("respool: lease not found")
	ErrLeaseAlreadyReleased  = errors.New("respool: lease already released")
	ErrLeaseExpired          = errors.New("respool: lease expired")
	ErrPoolClosed            = errors.New("respool: pool closed")
)
