package auth

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

type fixedBarrier struct {
	index uint64
	err   error
}

func (barrier fixedBarrier) ReadIndex(_ context.Context, group string) (uint64, error) {
	if group != raft.RootGroupID {
		return 0, errors.New("wrong Group")
	}
	return barrier.index, barrier.err
}

func applyAuthCommand(t *testing.T, state *rootstate.State, command rootstate.Command) {
	t.Helper()
	command.Version = rootstate.FormatVersion
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Apply(state.AppliedIndex()+1, encoded); err != nil {
		t.Fatal(err)
	}
	result, found, err := state.Result(command.RequestID)
	if err != nil || !found || result.Error != "" {
		t.Fatal("Root rejected auth command", result, found, err)
	}
}

func TestTenantTokenVerifiedAgainstCurrentRootAndTenant(t *testing.T) {
	now := time.Unix(1000, 0)
	token, credential, err := Generate("tenant-a", now, time.Hour)
	if err != nil || strings.Contains(credential.TokenSHA256, token) {
		t.Fatal("Token generation failed or leaked plaintext", err)
	}
	state, err := rootstate.Open(filepath.Join(t.TempDir(), "root"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	applyAuthCommand(t, state, rootstate.Command{RequestID: "tenant-a", Type: rootstate.RegisterTenant,
		Tenant: &rootstate.Tenant{TenantID: "tenant-a"}})
	applyAuthCommand(t, state, rootstate.Command{RequestID: "credential-a", Type: rootstate.CreateTenantCredential,
		TenantCredential: &credential})
	verifier := Verifier{Barrier: fixedBarrier{index: state.AppliedIndex()}, Catalog: state,
		Now: func() time.Time { return now.Add(time.Minute) }}
	principal, err := verifier.Verify(context.Background(), token)
	if err != nil || principal.TenantID() != "tenant-a" || principal.CredentialID() != credential.CredentialID {
		t.Fatal("current Root credential was not authenticated", principal, err)
	}
	if err := principal.Authorize(rootstate.Keyspace{TenantID: "tenant-a", CapabilityID: "record", DatasetID: "items"}); err != nil {
		t.Fatal("Tenant denied own Keyspace", err)
	}
	if err := principal.Authorize(rootstate.Keyspace{TenantID: "tenant-b", CapabilityID: "record", DatasetID: "items"}); !errors.Is(err, ErrForbiddenTenant) {
		t.Fatal("Tenant crossed Keyspace", err)
	}
	altered := token[:len(token)-1] + "A"
	if altered == token {
		altered = token[:len(token)-1] + "B"
	}
	if _, err := verifier.Verify(context.Background(), altered); !errors.Is(err, ErrInvalidCredential) {
		t.Fatal("wrong Token was accepted", err)
	}
	verifier.Barrier = fixedBarrier{index: state.AppliedIndex() + 1}
	if _, err := verifier.Verify(context.Background(), token); !errors.Is(err, ErrAuthorizationUnavailable) {
		t.Fatal("stale Root authorization view was accepted", err)
	}
	verifier.Barrier = fixedBarrier{err: errors.New("Root quorum lost")}
	if _, err := verifier.Verify(context.Background(), token); !errors.Is(err, ErrAuthorizationUnavailable) {
		t.Fatal("credential was accepted without Root majority", err)
	}
	applyAuthCommand(t, state, rootstate.Command{RequestID: "revoke-a", Type: rootstate.RevokeTenantCredential,
		TenantRevocation: &rootstate.TenantCredentialRevocation{CredentialID: credential.CredentialID}})
	verifier.Barrier = fixedBarrier{index: state.AppliedIndex()}
	if _, err := verifier.Verify(context.Background(), token); !errors.Is(err, ErrInvalidCredential) {
		t.Fatal("revoked Token was accepted", err)
	}
}
