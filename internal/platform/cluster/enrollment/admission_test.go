package enrollment

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

type admissionFixture struct {
	issuer     *Issuer
	authorizer *GrantAuthorizer
	host       *raft.Host
	root       *rootstate.State
	grant      rootstate.JoinGrant
	proof      []byte
	ctx        context.Context
	now        time.Time
	clock      time.Time
}

func admissionContext(t *testing.T, issuer *Issuer, grantID string, public []byte, expires, now time.Time) context.Context {
	t.Helper()
	encoded, err := issuer.signer.IssueEnrollment(grantID, public, expires, now)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(encoded)
	if block == nil {
		t.Fatal("no enrollment certificate")
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	block, _ = pem.Decode(issuer.signer.CACertificatePEM())
	if block == nil {
		t.Fatal("no CA")
	}
	ca, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	chains, err := leaf.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return peer.NewContext(ctx, &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: chains}}})
}

func submitAdmissionCommand(t *testing.T, f *admissionFixture, command rootstate.Command) {
	t.Helper()
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.host.Submit(raft.RootGroupID, encoded); err != nil {
		t.Fatal(err)
	}
	if _, err := f.host.ReadIndex(f.ctx, raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	result, found, err := f.root.ResultForCommand(encoded)
	if err != nil || !found || result.Error != "" {
		t.Fatal("Root command failed", result, found, err)
	}
}

func newAdmissionFixture(t *testing.T) *admissionFixture {
	t.Helper()
	issuer, root, host, id := openTestIssuer(t, func(context.Context, identityenrollment.Request, []byte) error { return errors.New("unset authorizer") })
	now := time.Now().UTC().Truncate(time.Second)
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	publicDER, err := x509.MarshalPKIXPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	secret := bytes.Repeat([]byte{42}, 32)
	secretDigest, publicDigest := sha256.Sum256(secret), sha256.Sum256(publicDER)
	grant := rootstate.JoinGrant{GrantID: "join_00000000000000000000000000000001", IssuerRaftID: id.RaftID,
		SecretSHA256: hex.EncodeToString(secretDigest[:]), EnrollmentKeySHA256: hex.EncodeToString(publicDigest[:]),
		CreatedUnix: now.Unix(), ExpiresUnix: now.Add(rootstate.DefaultJoinGrantTTL).Unix(), State: rootstate.JoinGrantActive}
	f := &admissionFixture{issuer: issuer, host: host, root: root, grant: grant, now: now, clock: now}
	f.ctx = admissionContext(t, issuer, grant.GrantID, publicDER, time.Unix(grant.ExpiresUnix, 0), now)
	f.proof, err = EncodeGrantProof(grant.GrantID, secret)
	if err != nil {
		t.Fatal(err)
	}
	f.authorizer, err = NewGrantAuthorizer(host, root, id.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	f.authorizer.now = func() time.Time { return f.clock }
	issuer.authorizer = f.authorizer
	submitAdmissionCommand(t, f, rootstate.Command{Version: 1, RequestID: "register-issuer", Type: rootstate.RegisterNode,
		Node: &rootstate.Node{NodeID: id.NodeID, RaftID: id.RaftID, RegionID: "r1", Address: "issuer:7443"}})
	submitAdmissionCommand(t, f, rootstate.Command{Version: 1, RequestID: "create-grant", Type: rootstate.CreateJoinGrant, JoinGrant: &grant})
	return f
}

func admissionRequest(t *testing.T) identityenrollment.Request {
	t.Helper()
	_, request, err := identityenrollment.Prepare(t.TempDir(), nodepki.DefaultServerName, "r1", "node:7443")
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func TestJoinGrantAdmissionConcurrentUseAndSameRequestRetry(t *testing.T) {
	for _, same := range []bool{false, true} {
		name := "different-requests"
		if same {
			name = "same-request"
		}
		t.Run(name, func(t *testing.T) {
			f := newAdmissionFixture(t)
			requests := []identityenrollment.Request{admissionRequest(t), admissionRequest(t)}
			if same {
				requests[1] = requests[0]
			}
			var wg sync.WaitGroup
			results := make([]error, len(requests))
			for i := range requests {
				wg.Add(1)
				go func(i int) { defer wg.Done(); _, results[i] = f.issuer.Issue(f.ctx, requests[i], f.proof, f.now) }(i)
			}
			wg.Wait()
			successes, winner := 0, -1
			for i, err := range results {
				if err == nil {
					successes++
					winner = i
				}
			}
			want := 1
			if same {
				want = 2
			}
			if successes != want {
				t.Fatal("wrong admitted count", successes, results)
			}
			bound, found, err := f.root.JoinGrant(f.grant.GrantID)
			encoded, _ := identityenrollment.EncodeRequest(requests[winner])
			digest := sha256.Sum256(encoded)
			if err != nil || !found || bound.State != rootstate.JoinGrantConsumed || bound.BoundRequestSHA256 != hex.EncodeToString(digest[:]) {
				t.Fatal("Root binding differs", bound, found, err)
			}
			for i, request := range requests {
				_, found, err := f.root.Node(request.RaftID)
				if err != nil || found != (same || i == winner) {
					t.Fatal("losing request registered a Node", i, found, err)
				}
			}
			if _, err := f.issuer.Issue(f.ctx, requests[winner], f.proof, f.now.Add(time.Minute)); err != nil {
				t.Fatal("same request retry failed", err)
			}
			changed := requests[winner]
			changed.RegionID = "other"
			if _, err := f.issuer.Issue(f.ctx, changed, f.proof, f.now); err == nil {
				t.Fatal("bound request changed")
			}
			// Even a verified, long-lived TLS session cannot reuse a consumed ticket
			// as an unbounded leaf-certificate renewal credential.
			f.clock = time.Unix(f.grant.ExpiresUnix, 0)
			if _, err := f.issuer.Issue(f.ctx, requests[winner], f.proof, f.clock); err == nil {
				t.Fatal("expired consumed Grant renewed certificate")
			}
		})
	}
}

type admissionHostFault struct {
	Host
	afterRead               func()
	failRegistration        bool
	loseConsumptionResponse bool
}

func (h admissionHostFault) Submit(group string, encoded []byte) error {
	var command rootstate.Command
	if err := json.Unmarshal(encoded, &command); err != nil {
		return err
	}
	if h.failRegistration && command.Type == rootstate.RegisterNode {
		return errors.New("registration temporarily unavailable")
	}
	err := h.Host.Submit(group, encoded)
	if err == nil && h.loseConsumptionResponse && command.Type == rootstate.ConsumeJoinGrant {
		return errors.New("consumption response lost")
	}
	return err
}
func (h admissionHostFault) ReadIndex(ctx context.Context, group string) (uint64, error) {
	index, err := h.Host.ReadIndex(ctx, group)
	if h.afterRead != nil {
		h.afterRead()
	}
	return index, err
}

func TestJoinGrantAdmissionRecoversAfterConsumptionBeforeRegistration(t *testing.T) {
	f := newAdmissionFixture(t)
	request := admissionRequest(t)
	f.issuer.host = admissionHostFault{Host: f.host, failRegistration: true}
	if _, err := f.issuer.Issue(f.ctx, request, f.proof, f.now); err == nil {
		t.Fatal("registration fault ignored")
	}
	grant, _, err := f.root.JoinGrant(f.grant.GrantID)
	if err != nil || grant.State != rootstate.JoinGrantConsumed {
		t.Fatal("consumption not durable", grant, err)
	}
	if _, found, err := f.root.Node(request.RaftID); err != nil || found {
		t.Fatal("failed registration issued Node", found, err)
	}
	f.issuer.host = f.host
	if _, err := f.issuer.Issue(f.ctx, request, f.proof, f.now); err != nil {
		t.Fatal("same bound request did not resume", err)
	}
}

func TestJoinGrantAdmissionLostResponseAndExpiryDuringRead(t *testing.T) {
	t.Run("lost-consumption-response", func(t *testing.T) {
		f := newAdmissionFixture(t)
		request := admissionRequest(t)
		f.authorizer.host = admissionHostFault{Host: f.host, loseConsumptionResponse: true}
		if _, err := f.issuer.Issue(f.ctx, request, f.proof, f.now); err != nil {
			t.Fatal("durable binding was not recovered", err)
		}
	})
	t.Run("expired-during-ReadIndex", func(t *testing.T) {
		f := newAdmissionFixture(t)
		request := admissionRequest(t)
		f.authorizer.host = admissionHostFault{Host: f.host, afterRead: func() { f.clock = time.Unix(f.grant.ExpiresUnix, 0) }}
		if _, err := f.issuer.Issue(f.ctx, request, f.proof, f.now); err == nil {
			t.Fatal("stale pre-quorum time authorized request")
		}
		grant, _, err := f.root.JoinGrant(f.grant.GrantID)
		if err != nil || grant.State != rootstate.JoinGrantActive {
			t.Fatal("expired read consumed Grant", grant, err)
		}
	})
}

func TestJoinGrantAdmissionRejectsWrongProofRoleAndRevocation(t *testing.T) {
	for _, damage := range []string{"no-TLS", "nil-peer", "wrong-secret", "wrong-key", "wrong-expiry", "expired", "revoked", "trailing", "unknown-field"} {
		t.Run(damage, func(t *testing.T) {
			f := newAdmissionFixture(t)
			request := admissionRequest(t)
			ctx, proof := f.ctx, bytes.Clone(f.proof)
			switch damage {
			case "no-TLS":
				ctx = context.Background()
			case "nil-peer":
				ctx = peer.NewContext(context.Background(), nil)
			case "wrong-secret":
				proof, _ = EncodeGrantProof(f.grant.GrantID, bytes.Repeat([]byte{43}, 32))
			case "wrong-key", "wrong-expiry":
				public, _, err := ed25519.GenerateKey(rand.Reader)
				if err != nil {
					t.Fatal(err)
				}
				der, err := x509.MarshalPKIXPublicKey(public)
				if err != nil {
					t.Fatal(err)
				}
				expiry := time.Unix(f.grant.ExpiresUnix, 0)
				if damage == "wrong-expiry" {
					remote, _ := peer.FromContext(ctx)
					leaf := remote.AuthInfo.(credentials.TLSInfo).State.PeerCertificates[0]
					der, err = x509.MarshalPKIXPublicKey(leaf.PublicKey)
					if err != nil {
						t.Fatal(err)
					}
					expiry = expiry.Add(time.Minute)
				}
				ctx = admissionContext(t, f.issuer, f.grant.GrantID, der, expiry, f.now)
			case "expired":
				f.clock = time.Unix(f.grant.ExpiresUnix, 0)
			case "revoked":
				submitAdmissionCommand(t, f, rootstate.Command{Version: 1, RequestID: "revoke", Type: rootstate.RevokeJoinGrant,
					JoinRevocation: &rootstate.JoinGrantRevocation{GrantID: f.grant.GrantID, RevokedUnix: f.now.Unix()}})
			case "trailing":
				proof = append(proof, []byte(" {}")...)
			case "unknown-field":
				proof = []byte(`{"grant_id":"join_00000000000000000000000000000001","unexpected":true}`)
			}
			if _, err := f.issuer.Issue(ctx, request, proof, f.now); err == nil {
				t.Fatal("invalid admission issued certificate")
			}
			if _, found, err := f.root.Node(request.RaftID); err != nil || found {
				t.Fatal("invalid admission registered Node", found, err)
			}
			grant, _, err := f.root.JoinGrant(f.grant.GrantID)
			if err != nil || grant.State == rootstate.JoinGrantConsumed {
				t.Fatal("invalid admission consumed Grant", grant, err)
			}
		})
	}
}
