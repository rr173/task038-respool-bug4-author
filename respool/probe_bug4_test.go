package respool

import "testing"

func TestProbeReclaimReturnsEmptySlice(t *testing.T) {
	p, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if reclaimed := p.Reclaim(); reclaimed == nil {
		t.Fatal("Reclaim returned nil; callers require an empty result slice")
	}
}
