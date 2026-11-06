package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"strings"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

type AccessCatalog interface {
	AppliedIndex() uint64
	AccessCredential(string) (rootstate.AccessCredential, bool, error)
}
type AccessVerifier struct {
	Barrier RootBarrier
	Catalog AccessCatalog
	Now     func() time.Time
}

func GenerateAccess(role, parent string, now time.Time, lifetime time.Duration) (string, rootstate.AccessCredential, error) {
	token, c, err := Generate("access", now, lifetime)
	if err != nil {
		return "", rootstate.AccessCredential{}, err
	}
	token = "dubi-access." + strings.TrimPrefix(token, "dubi.")
	digest := sha256.Sum256([]byte(token))
	v := rootstate.AccessCredential{CredentialID: c.CredentialID, Role: role, ParentID: parent, TokenSHA256: hex.EncodeToString(digest[:]), CreatedUnix: c.CreatedUnix, ExpiresUnix: c.ExpiresUnix}
	return token, v, v.Validate()
}
func (v AccessVerifier) Verify(ctx context.Context, token string) (rootstate.AccessCredential, error) {
	if !strings.HasPrefix(token, "dubi-access.") {
		return rootstate.AccessCredential{}, ErrInvalidCredential
	}
	id, err := tokenID("dubi." + strings.TrimPrefix(token, "dubi-access."))
	if err != nil {
		return rootstate.AccessCredential{}, err
	}
	if v.Barrier == nil || v.Catalog == nil {
		return rootstate.AccessCredential{}, ErrAuthorizationUnavailable
	}
	index, err := v.Barrier.ReadIndex(ctx, raft.RootGroupID)
	if err != nil || index == 0 || v.Catalog.AppliedIndex() < index {
		return rootstate.AccessCredential{}, ErrAuthorizationUnavailable
	}
	c, found, err := v.Catalog.AccessCredential(id)
	if err != nil {
		return rootstate.AccessCredential{}, ErrAuthorizationUnavailable
	}
	now := time.Now()
	if v.Now != nil {
		now = v.Now()
	}
	valid := func(c rootstate.AccessCredential) bool {
		return c.Validate() == nil && c.RevokedIndex == 0 && now.Unix() >= c.CreatedUnix && now.Unix() < c.ExpiresUnix
	}
	if !found || c.CredentialID != id || !valid(c) {
		return rootstate.AccessCredential{}, ErrInvalidCredential
	}
	digest := sha256.Sum256([]byte(token))
	want, _ := hex.DecodeString(c.TokenSHA256)
	if subtle.ConstantTimeCompare(digest[:], want) != 1 {
		return rootstate.AccessCredential{}, ErrInvalidCredential
	}
	if c.ParentID != "" {
		p, ok, err := v.Catalog.AccessCredential(c.ParentID)
		if err != nil {
			return rootstate.AccessCredential{}, ErrAuthorizationUnavailable
		}
		if !ok || p.Role != "admin" || !valid(p) {
			return rootstate.AccessCredential{}, ErrInvalidCredential
		}
	}
	return c, nil
}
