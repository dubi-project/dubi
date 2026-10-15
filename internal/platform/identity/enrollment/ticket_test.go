package enrollment

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
)

func ticketFixture(t testing.TB) (*GrantDraft, Ticket, *nodepki.Signer, time.Time) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nodepki.Ensure(store, id, nodepki.DefaultServerName, now); err != nil {
		t.Fatal(err)
	}
	signer, err := nodepki.OpenSigner(store, id, now)
	if err != nil {
		t.Fatal(err)
	}
	draft, err := NewGrantDraft(0, now)
	if err != nil {
		t.Fatal(err)
	}
	request := draft.Request()
	cert, err := signer.IssueEnrollment(request.GrantID, request.PublicKeyDER, time.Unix(request.ExpiresUnix, 0), now)
	if err != nil {
		t.Fatal(err)
	}
	ticket, err := draft.Complete(GrantBundle{ClusterID: id.ClusterID, GrantID: request.GrantID,
		IssuerAddress: "127.0.0.1:7443", ExpiresUnix: request.ExpiresUnix,
		CACertificatePEM: string(signer.CACertificatePEM()), CertificatePEM: string(cert)}, now)
	if err != nil {
		t.Fatal(err)
	}
	return draft, ticket, signer, now
}

func TestJoinTicketRoundTripPinnedTLSAndRedaction(t *testing.T) {
	draft, ticket, _, now := ticketFixture(t)
	if err := draft.Request().Validate(now); err != nil {
		t.Fatal(err)
	}
	copy := draft.Request()
	copy.PublicKeyDER[0] ^= 1
	if err := draft.Request().Validate(now); err != nil {
		t.Fatal("mutable draft request", err)
	}
	encoded, err := EncodeTicket(ticket, now)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeTicket(encoded, now)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ClusterID() != ticket.ClusterID() || decoded.Address() != ticket.Address() || decoded.GrantID() != ticket.GrantID() {
		t.Fatal("ticket identity changed")
	}
	reencoded, err := EncodeTicket(decoded, now)
	if err != nil || reencoded != encoded {
		t.Fatal("nonstable encoding", err)
	}
	config, err := decoded.ClientTLS(now)
	if err != nil {
		t.Fatal(err)
	}
	if config.InsecureSkipVerify || config.MinVersion != tls.VersionTLS13 || config.ServerName != nodepki.DefaultServerName ||
		config.RootCAs == nil || len(config.RootCAs.Subjects()) != 1 || len(config.Certificates) != 1 {
		t.Fatal("un-pinned TLS")
	}
	if _, err := identity.ParseNodeCertificate(config.Certificates[0].Leaf); err == nil {
		t.Fatal("ticket became Node")
	}
	proof, err := ticket.AdmissionProof(now)
	if err != nil || !bytes.Contains(proof, []byte(ticket.GrantID())) {
		t.Fatal("missing proof", err)
	}
	for _, value := range []any{draft, *draft, &ticket, ticket} {
		for _, format := range []string{"%v", "%+v", "%#v"} {
			if got := fmt.Sprintf(format, value); !strings.Contains(got, "redacted") || strings.Contains(got, "PRIVATE KEY") {
				t.Fatal("secret printable", got)
			}
		}
		encoded, err := json.Marshal(value)
		if err != nil || string(encoded) != "{}" {
			t.Fatal("implicit JSON leaked secret", err)
		}
	}
	for _, invalid := range []time.Duration{-time.Second, time.Nanosecond, identity.MaxJoinGrantTTL + time.Second} {
		if _, err := NewGrantDraft(invalid, now); err == nil {
			t.Fatal("invalid TTL accepted")
		}
	}
}

func TestJoinTicketRejectsAmbiguityAndCredentialReplacement(t *testing.T) {
	draft, ticket, _, now := ticketFixture(t)
	otherDraft, other, _, _ := ticketFixture(t)
	for name, mutate := range map[string]func(*ticketDocument){
		"version":          func(d *ticketDocument) { d.Version++ },
		"cluster":          func(d *ticketDocument) { d.ClusterID = other.ClusterID() },
		"grant":            func(d *ticketDocument) { d.GrantID = other.GrantID() },
		"secret-size":      func(d *ticketDocument) { d.Secret = []byte("short") },
		"key":              func(d *ticketDocument) { d.PrivateKeyPEM = other.document.PrivateKeyPEM },
		"expiry":           func(d *ticketDocument) { d.ExpiresUnix++ },
		"ca":               func(d *ticketDocument) { d.CACertificatePEM = other.document.CACertificatePEM },
		"two-leaves":       func(d *ticketDocument) { d.CertificatePEM += d.CertificatePEM },
		"two-keys":         func(d *ticketDocument) { d.PrivateKeyPEM += d.PrivateKeyPEM },
		"junk-leaf-prefix": func(d *ticketDocument) { d.CertificatePEM = "junk\n" + d.CertificatePEM },
		"junk-ca-prefix":   func(d *ticketDocument) { d.CACertificatePEM = "junk\n" + d.CACertificatePEM },
		"junk-key-prefix":  func(d *ticketDocument) { d.PrivateKeyPEM = "junk\n" + d.PrivateKeyPEM },
		"missing-port":     func(d *ticketDocument) { d.IssuerAddress = "example.internal" },
		"resolver":         func(d *ticketDocument) { d.IssuerAddress = "dns:///example.internal:7443" },
		"userinfo":         func(d *ticketDocument) { d.IssuerAddress = "user@host:7443" },
		"unspecified":      func(d *ticketDocument) { d.IssuerAddress = "0.0.0.0:7443" },
		"zero-port":        func(d *ticketDocument) { d.IssuerAddress = "host:0" },
		"multicast":        func(d *ticketDocument) { d.IssuerAddress = "[ff02::1]:7443" },
	} {
		t.Run(name, func(t *testing.T) {
			copy := ticket.document
			mutate(&copy)
			encoded, err := json.Marshal(copy)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeTicket(joinTicketPrefix+base64.RawURLEncoding.EncodeToString(encoded), now); err == nil {
				t.Fatal("invalid ticket accepted")
			}
		})
	}
	canonical, _ := json.Marshal(ticket.document)
	for _, malformed := range [][]byte{append(bytes.Clone(canonical), '\n'),
		append([]byte(`{"version":1,`), canonical[1:]...),
		append([]byte(`{"extra":true,`), canonical[1:]...), append(bytes.Clone(canonical), []byte(`{}`)...)} {
		if _, err := DecodeTicket(joinTicketPrefix+base64.RawURLEncoding.EncodeToString(malformed), now); err == nil {
			t.Fatal("ambiguous document accepted")
		}
	}
	encoded, _ := EncodeTicket(ticket, now)
	for _, malformed := range []string{encoded + "=", " " + encoded, strings.Repeat("x", MaxJoinTicketSize+1), "djt2." + encoded[5:]} {
		if _, err := DecodeTicket(malformed, now); err == nil {
			t.Fatal("ambiguous encoding accepted")
		}
	}
	expired := time.Unix(draft.Request().ExpiresUnix, 0)
	if _, err := DecodeTicket(encoded, expired); err == nil {
		t.Fatal("expired ticket accepted")
	}
	if _, err := ticket.ClientTLS(expired); err == nil {
		t.Fatal("expired TLS allowed")
	}
	if _, err := ticket.AdmissionProof(expired); err == nil {
		t.Fatal("expired proof allowed")
	}
	if _, err := otherDraft.Complete(ticket.document.GrantBundle, now); err == nil {
		t.Fatal("wrong draft completed")
	}
	wrongKey := ticket.document.GrantBundle
	wrongKey.CertificatePEM = other.document.CertificatePEM
	if _, err := draft.Complete(wrongKey, now); err == nil {
		t.Fatal("wrong certificate completed")
	}
}

func TestJoinTicketPinsNodeResponseToClusterAndCSR(t *testing.T) {
	_, ticket, signer, now := ticketFixture(t)
	_, request, err := Prepare(t.TempDir(), nodepki.DefaultServerName, "local", "127.0.0.1:7444")
	if err != nil {
		t.Fatal(err)
	}
	bound := identity.Identity{Version: 1, NodeID: request.NodeID, RaftID: request.RaftID, ClusterID: ticket.ClusterID(), CreatedAt: now}
	cert, err := signer.IssueNode(bound, request.ServerName, []byte(request.CSRPEM), now)
	if err != nil {
		t.Fatal(err)
	}
	bundle := Bundle{Version: FormatVersion, ClusterID: bound.ClusterID, NodeID: bound.NodeID, RaftID: bound.RaftID,
		ServerName: request.ServerName, CACertificatePEM: ticket.document.CACertificatePEM, CertificatePEM: string(cert)}
	if err := ticket.ValidateNodeBundle(bundle, request, now); err != nil {
		t.Fatal(err)
	}
	_, other, otherSigner, _ := ticketFixture(t)
	for name, mutate := range map[string]func(*Bundle){
		"node": func(b *Bundle) { b.NodeID = "node_00000000000000000000000000000001" },
		"raft": func(b *Bundle) { b.RaftID++ },
		"name": func(b *Bundle) { b.ServerName = "unexpected.internal" },
		"ca":   func(b *Bundle) { b.CACertificatePEM = other.document.CACertificatePEM },
		"cluster": func(b *Bundle) {
			b.ClusterID = other.ClusterID()
			b.CACertificatePEM = other.document.CACertificatePEM
			copy := bound
			copy.ClusterID = b.ClusterID
			cert, err := otherSigner.IssueNode(copy, request.ServerName, []byte(request.CSRPEM), now)
			if err != nil {
				t.Fatal(err)
			}
			b.CertificatePEM = string(cert)
		},
	} {
		t.Run(name, func(t *testing.T) {
			copy := bundle
			mutate(&copy)
			if err := ticket.ValidateNodeBundle(copy, request, now); err == nil {
				t.Fatal("mismatched response accepted")
			}
		})
	}
	if err := ticket.ValidateNodeBundle(bundle, Request{}, now); err == nil {
		t.Fatal("missing request accepted")
	}
}

func FuzzJoinTicketDecode(f *testing.F) {
	_, ticket, _, now := ticketFixture(f)
	encoded, err := EncodeTicket(ticket, now)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(encoded)
	f.Add("djt1.")
	f.Add("djt1.e30")
	f.Add("djt1.bm90LWpzb24")
	f.Fuzz(func(t *testing.T, encoded string) {
		ticket, err := DecodeTicket(encoded, now)
		if err != nil {
			return
		}
		canonical, err := EncodeTicket(ticket, now)
		if err != nil || canonical != encoded {
			t.Fatal("accepted noncanonical ticket")
		}
	})
}
