package vrr

import (
	"testing"
)

func TestHarnessBasic(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	h.CheckSinglePrimary()
}
