package iop

import (
	"testing"
	"time"
)

// Regression tests for the ready-signal deadlock that hung sling replications
// in production (5 occurrences between 2026-07-18 and 2026-07-31).
//
// The bug: SetReady() delivered readiness by SENDING a single token into a
// buffered(1) channel, guarded so it could only ever fire once. Close() then
// DRAINED that same token during its cleanup. Any waiter that arrived after
// the close blocked forever, because the token was gone and SetReady() could
// never fire again.
//
// In production the waiter was Dataflow.PushStreamChan (`case <-ds.readyChn`),
// which therefore never reached its trailing df.SetReady() — so BulkExportFlow's
// df.WaitReady() blocked too, and the whole `sling run` process parked on
// wg.Wait() holding its flock. Streams that return few or no rows close almost
// immediately after signalling ready, which is why only tiny streams ever hit it.
//
// The fix makes readiness a BROADCAST (close the channel, once) instead of a
// consumable token, so it cannot be "used up" by anyone.

// waitForLegacyToken gives SetReady's sender goroutine a chance to actually
// deliver its token, so an unfixed build reproduces the bug deterministically
// rather than racing. On a fixed build readyChn is closed and len() stays 0,
// so this simply falls through after a bounded wait.
func waitForLegacyToken(ds *Datastream) {
	for i := 0; i < 200 && len(ds.readyChn) == 0; i++ {
		time.Sleep(time.Millisecond)
	}
}

// mustReceiveReady asserts a waiter can observe readiness, the way
// Dataflow.PushStreamChan does. Fails instead of hanging the suite.
func mustReceiveReady(t *testing.T, ds *Datastream, what string) {
	t.Helper()
	select {
	case <-ds.readyChn:
	case <-time.After(5 * time.Second):
		t.Fatalf("deadlock: %s — readyChn never fired; a waiter would block forever", what)
	}
}

// TestDatastreamReadyAfterClose is the direct reproduction of the production
// hang: ready, then closed, then a waiter arrives.
func TestDatastreamReadyAfterClose(t *testing.T) {
	ds := NewDatastream(nil)
	ds.SetReady()
	waitForLegacyToken(ds)
	ds.Close()

	mustReceiveReady(t, ds, "SetReady() then Close() then wait")
}

// TestDatastreamReadyIsBroadcast proves readiness is observable by more than
// one waiter. A single-token channel can only ever satisfy the first.
func TestDatastreamReadyIsBroadcast(t *testing.T) {
	ds := NewDatastream(nil)
	ds.SetReady()
	waitForLegacyToken(ds)

	for i := 0; i < 3; i++ {
		mustReceiveReady(t, ds, "concurrent waiter")
	}
}

// TestDatastreamCloseWithoutReadyUnblocks covers a stream that is closed
// before it ever became ready (empty/aborted source). A waiter must still be
// released rather than parking forever.
func TestDatastreamCloseWithoutReadyUnblocks(t *testing.T) {
	ds := NewDatastream(nil)
	ds.Close()

	mustReceiveReady(t, ds, "Close() without SetReady()")
}

// TestDatastreamSetReadyIdempotent guards the fix itself: readiness is
// delivered by closing a channel, so a second SetReady() must not panic with
// "close of closed channel". Close() also signals readiness, so the
// SetReady/Close/SetReady ordering below is a real production sequence.
func TestDatastreamSetReadyIdempotent(t *testing.T) {
	ds := NewDatastream(nil)
	ds.SetReady()
	ds.SetReady()
	ds.Close()
	ds.SetReady()

	mustReceiveReady(t, ds, "repeated SetReady()")
}

// TestDatastreamSetReadyConcurrent runs SetReady() from many goroutines at
// once. The old `if !ds.Ready` check-then-act was racy; the fix must be
// safe under -race and must not double-close.
func TestDatastreamSetReadyConcurrent(t *testing.T) {
	ds := NewDatastream(nil)

	start := make(chan struct{})
	done := make(chan struct{}, 16)
	for i := 0; i < 16; i++ {
		go func() {
			<-start
			ds.SetReady()
			done <- struct{}{}
		}()
	}
	close(start)
	for i := 0; i < 16; i++ {
		<-done
	}

	mustReceiveReady(t, ds, "concurrent SetReady()")
}

// TestDataflowPushStreamChanAfterDatastreamClose reproduces the production
// call shape end to end: BaseConn.BulkExportFlow feeds PushStreamChan and then
// blocks in df.WaitReady(). With the token-based signal, a datastream that
// closed before PushStreamChan reached its select stalled both goroutines —
// exactly the pair seen parked for 14 minutes in the 2026-07-31 dump
// (dataflow.go:633 and dataflow.go:704 on the same Dataflow).
func TestDataflowPushStreamChanAfterDatastreamClose(t *testing.T) {
	df := NewDataflow()

	ds := NewDatastream(nil)
	ds.SetReady()
	waitForLegacyToken(ds)
	ds.Close()

	dsCh := make(chan *Datastream, 1)
	dsCh <- ds
	close(dsCh)

	go df.PushStreamChan(dsCh)

	done := make(chan error, 1)
	go func() { done <- df.WaitReady() }()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: df.WaitReady() never returned — PushStreamChan is parked on ds.readyChn")
	}
}

// TestDataflowSetReadyIdempotent covers the same signalling pattern on
// Dataflow. PushStreamChan calls df.SetReady() from four different places, so
// it must be safe to call repeatedly.
func TestDataflowSetReadyIdempotent(t *testing.T) {
	df := NewDataflow()
	df.SetReady()
	df.SetReady()
	df.SetReady()

	select {
	case <-df.readyChn:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: df.readyChn never fired after repeated SetReady()")
	}
}
