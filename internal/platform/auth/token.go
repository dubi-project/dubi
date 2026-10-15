// Package auth authenticates application credentials and binds requests to a
// Tenant. It does not grant Node control-plane authority.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

const tokenPrefix = "dubi."
const tokenSecretBytes = 32

var ErrInvalidCredential = errors.New("invalid Tenant credential")
var ErrAuthorizationUnavailable = errors.New("Tenant authorization is unavailable")
var ErrForbiddenTenant = errors.New("Tenant credential cannot access this Keyspace")

type Principal struct {
	tenantID     string
	credentialID string
}

func (principal Principal) TenantID() string     { return principal.tenantID }
func (principal Principal) CredentialID() string { return principal.credentialID }

func (principal Principal) Authorize(keyspace rootstate.Keyspace) error {
	if principal.tenantID == "" || principal.credentialID == "" || keyspace.Validate() != nil ||
		keyspace.TenantID != principal.tenantID {
		return ErrForbiddenTenant
	}
	return nil
}

// Generate creates a 256-bit one-time-display application Token. Only its
// SHA-256 digest may be submitted to Root. High entropy makes plain SHA-256
// sufficient for offline-guessing resistance; it is not a password hash.
func Generate(tenantID string, now time.Time, lifetime time.Duration) (string, rootstate.TenantCredential, error) {
	if rootstate.ValidateTenantID(tenantID) != nil || now.IsZero() || lifetime < time.Second ||
		lifetime > 366*24*time.Hour {
		return "", rootstate.TenantCredential{}, errors.New("invalid Tenant Token lifetime or TenantID")
	}
	var id [16]byte
	var secret [tokenSecretBytes]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", rootstate.TenantCredential{}, err
	}
	if _, err := rand.Read(secret[:]); err != nil {
		return "", rootstate.TenantCredential{}, err
	}
	credentialID := "cred_" + hex.EncodeToString(id[:])
	token := tokenPrefix + hex.EncodeToString(id[:]) + "." + base64.RawURLEncoding.EncodeToString(secret[:])
	digest := sha256.Sum256([]byte(token))
	value := rootstate.TenantCredential{CredentialID: credentialID, TenantID: tenantID,
		TokenSHA256: hex.EncodeToString(digest[:]), CreatedUnix: now.Unix(), ExpiresUnix: now.Add(lifetime).Unix()}
	if err := value.Validate(); err != nil {
		return "", rootstate.TenantCredential{}, err
	}
	return token, value, nil
}

func tokenID(token string) (string, error) {
	if len(token) != len(tokenPrefix)+32+1+43 || !strings.HasPrefix(token, tokenPrefix) {
		return "", ErrInvalidCredential
	}
	parts := strings.Split(token[len(tokenPrefix):], ".")
	if len(parts) != 2 || len(parts[0]) != 32 || len(parts[1]) != 43 {
		return "", ErrInvalidCredential
	}
	id, err := hex.DecodeString(parts[0])
	if err != nil || len(id) != 16 || hex.EncodeToString(id) != parts[0] {
		return "", ErrInvalidCredential
	}
	secret, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || len(secret) != tokenSecretBytes ||
		base64.RawURLEncoding.EncodeToString(secret) != parts[1] {
		return "", ErrInvalidCredential
	}
	return "cred_" + parts[0], nil
}

type RootBarrier interface {
	ReadIndex(context.Context, string) (uint64, error)
}

type CredentialCatalog interface {
	AppliedIndex() uint64
	TenantCredential(string) (rootstate.TenantCredential, bool, error)
}

type Verifier struct {
	Barrier RootBarrier
	Catalog CredentialCatalog
	Now     func() time.Time // optional clock; tests can supply a fixed value.
}

func (verifier Verifier) Verify(ctx context.Context, token string) (Principal, error) {
	credentialID, err := tokenID(token)
	if err != nil {
		return Principal{}, err
	}
	if ctx == nil || verifier.Barrier == nil || verifier.Catalog == nil {
		return Principal{}, ErrAuthorizationUnavailable
	}
	index, err := verifier.Barrier.ReadIndex(ctx, raft.RootGroupID)
	if err != nil || index == 0 || verifier.Catalog.AppliedIndex() < index {
		return Principal{}, fmt.Errorf("%w: Root read cut is not applied: %v", ErrAuthorizationUnavailable, err)
	}
	value, found, err := verifier.Catalog.TenantCredential(credentialID)
	if err != nil {
		return Principal{}, fmt.Errorf("%w: read Root credential: %v", ErrAuthorizationUnavailable, err)
	}
	now := time.Now()
	if verifier.Now != nil {
		now = verifier.Now()
	}
	if !found || value.Validate() != nil || value.CredentialID != credentialID ||
		value.RevokedIndex != 0 || now.Unix() < value.CreatedUnix || now.Unix() >= value.ExpiresUnix {
		return Principal{}, ErrInvalidCredential
	}
	want, err := hex.DecodeString(value.TokenSHA256)
	if err != nil {
		return Principal{}, ErrAuthorizationUnavailable
	}
	actual := sha256.Sum256([]byte(token))
	if subtle.ConstantTimeCompare(want, actual[:]) != 1 {
		return Principal{}, ErrInvalidCredential
	}
	return Principal{tenantID: value.TenantID, credentialID: value.CredentialID}, nil
}
