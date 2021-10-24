package vrr

import (
	"testing"
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
	origPrimaryID, _ := h.CheckSinglePrimary()

	h.DisconnectPeer(origPrimaryID)

	sleepMs(350)
	newPrimaryID, newView := h.CheckSinglePrimary()

	t.Logf("YOW %v %v", newPrimaryID, newView)

	h.ReconnectPeer(origPrimaryID)
	sleepMs(150)

	newPrimaryID, newView = h.CheckSinglePrimary()
	t.Logf("YOW %v %v", newPrimaryID, newView)

	h.ReportAll()
}
