package resourceid

import (
	"strings"
	"testing"
)

func TestCompactAndExistingIdentities(t *testing.T) {
	for _, prefix := range []string{"cluster_", "node_", "store_", "volume_"} {
		id, err := New(prefix)
		if err != nil || len(id) != len(prefix)+12 || !Valid(id, prefix) {
			t.Fatal(id, err)
		}
		if !Valid(prefix+strings.Repeat("a", 32), prefix) {
			t.Fatal("existing identity rejected")
		}
		for _, bad := range []string{prefix + strings.Repeat("a", 11), prefix + strings.Repeat("a", 13), prefix + strings.Repeat("a", 31), prefix + strings.Repeat("A", 12), prefix + strings.Repeat("z", 12), "wrong_" + strings.Repeat("a", 12)} {
			if Valid(bad, prefix) {
				t.Fatal("invalid identity accepted", bad)
			}
		}
	}
	if _, err := New("token_"); err == nil {
		t.Fatal("credential generation was shortened")
	}
}
