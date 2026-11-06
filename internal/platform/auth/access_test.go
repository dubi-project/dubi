package auth

import (
	"context"
	"errors"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"testing"
	"time"
)

func TestAccessCredentialIsolationParentRevocationAndSnapshot(t *testing.T) {
	now := time.Unix(2000, 0)
	state, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	token, admin, err := GenerateAccess("admin", "", now, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	sessionToken, session, err := GenerateAccess("session", admin.CredentialID, now, 12*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	applyAuthCommand(t, state, rootstate.Command{RequestID: "admin", Type: rootstate.CreateAccessCredential, AccessCredential: &admin})
	applyAuthCommand(t, state, rootstate.Command{RequestID: "session", Type: rootstate.CreateAccessCredential, AccessCredential: &session})
	verifier := AccessVerifier{Barrier: fixedBarrier{index: state.AppliedIndex()}, Catalog: state, Now: func() time.Time { return now.Add(time.Minute) }}
	for _, token := range []string{token, sessionToken} {
		if _, err := verifier.Verify(context.Background(), token); err != nil {
			t.Fatal(err)
		}
	}
	tenantToken, _, _ := Generate("tenant", now, time.Hour)
	if _, err := verifier.Verify(context.Background(), tenantToken); !errors.Is(err, ErrInvalidCredential) {
		t.Fatal("Tenant elevated", err)
	}
	if _, err := (Verifier{}).Verify(context.Background(), token); !errors.Is(err, ErrInvalidCredential) {
		t.Fatal("admin became Tenant", err)
	}
	image, index, conf, err := state.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if err = restored.Restore(index, image, conf); err != nil {
		t.Fatal(err)
	}
	verifier.Catalog = restored
	if _, err = verifier.Verify(context.Background(), sessionToken); err != nil {
		t.Fatal("snapshot session", err)
	}
	applyAuthCommand(t, restored, rootstate.Command{RequestID: "revoke", Type: rootstate.RevokeAccessCredential, AccessRevocation: &rootstate.TenantCredentialRevocation{CredentialID: admin.CredentialID}})
	verifier.Barrier = fixedBarrier{index: restored.AppliedIndex()}
	if _, err = verifier.Verify(context.Background(), sessionToken); !errors.Is(err, ErrInvalidCredential) {
		t.Fatal("parent revoke did not revoke session", err)
	}
	verifier.Barrier = fixedBarrier{index: restored.AppliedIndex() + 1}
	if _, err = verifier.Verify(context.Background(), token); !errors.Is(err, ErrAuthorizationUnavailable) {
		t.Fatal("stale auth accepted", err)
	}
}
