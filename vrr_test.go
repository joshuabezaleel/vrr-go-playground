package vrr

import (
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

func TestHarnessBasic(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	h.CheckSinglePrimary()
}

func TestViewChangePrimaryDisconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origPrimaryID, origView := h.CheckSinglePrimary()

	h.DisconnectPeer(origPrimaryID)
	sleepMs(350)

	newPrimaryID, newView := h.CheckSinglePrimary()
	if newPrimaryID == origPrimaryID {
		t.Errorf("want new primary to be different from orig primary, got %d and %d", newPrimaryID, origPrimaryID)
	}
	if newView <= origView {
		t.Errorf("want newView > origView, got %d and %d", newView, origView)
	}

	h.ReportAll()
}

func TestViewChangePrimaryDisconnectThenReconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	origPrimaryID, origView := h.CheckSinglePrimary()

	h.DisconnectPeer(origPrimaryID)

	sleepMs(350)
	newPrimaryID, newView := h.CheckSinglePrimary()

	h.ReconnectPeer(origPrimaryID)
	sleepMs(150)

	newPrimaryID, newView = h.CheckSinglePrimary()
	if newPrimaryID == origPrimaryID {
		t.Errorf("want new primary to be different from orig primary, got %d and %d", newPrimaryID, origPrimaryID)
	}
	if newView <= origView {
		t.Errorf("want newView > origView, got %d and %d", newView, origView)
	}

	// Replica[0] should have viewNum of 1 and primaryID 1
	// from State Transfer.
	h.ReportAll()
}

func TestCommitOneCommand(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	origPrimaryID, _ := h.CheckSinglePrimary()

	tlog("submitting 42 to %d", origPrimaryID)
	isPrimary, err := h.SubmitToReplica(origPrimaryID, Request{ClientID: 1, RequestNum: 1, Operation: 42})
	if !isPrimary {
		t.Errorf("want id=%d leader, but it's not", origPrimaryID)
	}

	if err != nil {
		t.Errorf("error = %+v", err)
	}

	sleepMs(150)

}

func TestGoroutineLeak(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	sleepMs(150)
}
