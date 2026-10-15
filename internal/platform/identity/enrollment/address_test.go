package enrollment

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
)

func TestEnrollmentNodeAddressValidationBeforeStoreCreation(t *testing.T) {
	bad := []string{"0.0.0.0:7443", "[::]:7443", "224.0.0.1:7443", "[ff02::1]:7443", "host:0", "host:07443", "host:65536", "host/path:7443", "user@host:7443", "host name:7443", "dns:///host:7443", "[fe80::1%en0]:7443", strings.Repeat("a", 64) + ":7443", strings.Repeat(strings.Repeat("a", 63)+".", 4) + "a:7443"}
	_, request, err := Prepare(t.TempDir(), nodepki.DefaultServerName, "local", "127.0.0.1:7443")
	if err != nil {
		t.Fatal(err)
	}
	for _, address := range bad {
		t.Run(address, func(t *testing.T) {
			store := filepath.Join(t.TempDir(), "uncreated")
			if _, _, err := Prepare(store, nodepki.DefaultServerName, "local", address); err == nil {
				t.Fatal("invalid advertise address accepted")
			}
			if _, err := os.Lstat(store); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("invalid address created identity or Store", err)
			}
			candidate := request
			candidate.Address = address
			if _, err := EncodeRequest(candidate); err == nil {
				t.Fatal("encoded invalid address")
			}
			encoded, err := json.Marshal(candidate)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeRequest(encoded); err == nil {
				t.Fatal("decoded invalid address")
			}
		})
	}
	for _, address := range []string{"127.0.0.1:7443", "localhost:7443", "n1.example:7443", "n1.example.:65535", "[::1]:7443", "[2001:db8::1]:7443"} {
		if err := ValidateNodeAddress(address); err != nil {
			t.Fatal("valid endpoint rejected", address, err)
		}
	}
}
