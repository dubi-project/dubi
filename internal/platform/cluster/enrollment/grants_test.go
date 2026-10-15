package enrollment

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

func grantManagerFixture(t *testing.T) (*GrantManager, *identityenrollment.GrantDraft, time.Time) {
	t.Helper()
	issuer, root, host, local := openTestIssuer(t, func(context.Context, identityenrollment.Request, []byte) error { return errors.New("unused") })
	manager, err := NewGrantManager(host, root, issuer.signer, local)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	manager.now = func() time.Time { return now }
	draft, err := identityenrollment.NewGrantDraft(0, now)
	if err != nil {
		t.Fatal(err)
	}
	command, err := rootstate.EncodeCommand(rootstate.Command{Version: 1, Type: rootstate.RegisterNode, RequestID: "register-issuer",
		Node: &rootstate.Node{NodeID: local.NodeID, RaftID: local.RaftID, RegionID: "local", Address: "127.0.0.1:7443"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Submit(raft.RootGroupID, command); err != nil {
		t.Fatal(err)
	}
	return manager, draft, now
}

type grantHostFault struct {
	Host
	loseType   string
	failSubmit bool
	afterRead  func()
}

func (h grantHostFault) Submit(group string, data []byte) error {
	if h.failSubmit {
		return errors.New("injected pre-proposal failure")
	}
	if err := h.Host.Submit(group, data); err != nil {
		return err
	}
	var command rootstate.Command
	if err := json.Unmarshal(data, &command); err != nil {
		return err
	}
	if command.Type == h.loseType {
		return errors.New("injected lost response after durable proposal")
	}
	return nil
}
func (h grantHostFault) ReadIndex(ctx context.Context, group string) (uint64, error) {
	index, err := h.Host.ReadIndex(ctx, group)
	if err == nil && h.afterRead != nil {
		h.afterRead()
	}
	return index, err
}

func TestJoinGrantManagerConcurrentCreateAndLostResponse(t *testing.T) {
	m, draft, now := grantManagerFixture(t)
	m.host = grantHostFault{Host: m.host, loseType: rootstate.CreateJoinGrant}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bundle, err := m.Create(ctx, draft.Request())
			if err != nil {
				t.Error(err)
				return
			}
			if _, err := draft.Complete(bundle, now); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	grant, found, err := m.root.JoinGrant(draft.Request().GrantID)
	if err != nil || !found || grant.State != rootstate.JoinGrantActive || grant.CreatedUnix != now.Unix() {
		t.Fatal(grant, found, err)
	}
	for _, field := range []string{"secret", "key", "expiry"} {
		request := draft.Request()
		switch field {
		case "secret":
			request.SecretSHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
		case "key":
			other, err := identityenrollment.NewGrantDraft(0, now)
			if err != nil {
				t.Fatal(err)
			}
			request.PublicKeyDER = other.Request().PublicKeyDER
		case "expiry":
			request.ExpiresUnix++
		}
		if _, err := m.Create(ctx, request); err == nil {
			t.Fatal("replaced existing grant", field)
		}
	}
	m.host = grantHostFault{Host: m.host, loseType: rootstate.RevokeJoinGrant}
	if err := m.Revoke(ctx, grant.GrantID); err != nil {
		t.Fatal("lost revocation response", err)
	}
	if err := m.Revoke(ctx, grant.GrantID); err != nil {
		t.Fatal("idempotent revoke", err)
	}
	if _, err := m.Create(ctx, draft.Request()); err == nil {
		t.Fatal("revoked grant reactivated")
	}
}

func TestJoinGrantManagerCannotReturnUnpublishedOrExpiredCredential(t *testing.T) {
	for _, mode := range []string{"failed-proposal", "expired-read", "missing-issuer"} {
		t.Run(mode, func(t *testing.T) {
			m, draft, now := grantManagerFixture(t)
			switch mode {
			case "failed-proposal":
				m.host = grantHostFault{Host: m.host, failSubmit: true}
			case "expired-read":
				m.host = grantHostFault{Host: m.host, afterRead: func() { m.now = func() time.Time { return now.Add(time.Hour) } }}
			case "missing-issuer":
				m.local.RaftID++
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := m.Create(ctx, draft.Request()); err == nil {
				t.Fatal("unsafe credential returned")
			}
			if _, found, err := m.root.JoinGrant(draft.Request().GrantID); err != nil || found {
				t.Fatal("invalid grant published", found, err)
			}
		})
	}
}

func TestJoinGrantManagerConsumedAndRevocationRace(t *testing.T) {
	m, draft, now := grantManagerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bundle, err := m.Create(ctx, draft.Request())
	if err != nil {
		t.Fatal(err)
	}
	ticket, err := draft.Complete(bundle, now)
	if err != nil {
		t.Fatal(err)
	}
	config, err := ticket.ClientTLS(now)
	if err != nil {
		t.Fatal(err)
	}
	leaf := config.Certificates[0].Leaf
	// A verified TLS peer is provided by admissionContext. This is a state
	// machine race test, not evidence of a network handshake.
	issuer := &Issuer{signer: m.signer}
	peerCtx := admissionContext(t, issuer, ticket.GrantID(), draft.Request().PublicKeyDER, leaf.NotAfter, now)
	authorizer, err := NewGrantAuthorizer(m.host, m.root, m.local.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	authorizer.now = func() time.Time { return now }
	proof, err := ticket.AdmissionProof(now)
	if err != nil {
		t.Fatal(err)
	}
	request := admissionRequest(t)
	var consumed, revoked error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); consumed = authorizer.Authorize(peerCtx, request, proof) }()
	go func() { defer wg.Done(); revoked = m.Revoke(ctx, ticket.GrantID()) }()
	wg.Wait()
	if (consumed == nil) == (revoked == nil) {
		t.Fatal("consumption/revocation must have one winner", consumed, revoked)
	}
	grant, found, err := m.root.JoinGrant(ticket.GrantID())
	if err != nil || !found {
		t.Fatal(found, err)
	}
	if consumed == nil && grant.State != rootstate.JoinGrantConsumed || revoked == nil && grant.State != rootstate.JoinGrantRevoked {
		t.Fatal("unexpected terminal state", grant.State)
	}
	if _, err := m.Create(ctx, draft.Request()); err == nil {
		t.Fatal("terminal grant returned as active")
	}
}
