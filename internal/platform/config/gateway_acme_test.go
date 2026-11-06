package config

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Exercises the real ACME client, HTTP-01 listener, CSR, durable replacement
// and restart cache without calling a public CA or changing the machine clock.
func TestGatewayManagedACMERenewalAndFailedCandidate(t *testing.T) {
	dir := t.TempDir()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	challengeAddress := listener.Addr().String()
	listener.Close()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	ca := &x509.Certificate{SerialNumber: big.NewInt(100), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour)}
	var mu sync.Mutex
	serial := int64(0)
	authorized := false
	wrongHost := false
	var chain []byte
	var endpoint string
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Replay-Nonce", "bm9uY2U")
		w.Header().Set("Content-Type", "application/json")
		reply := func(status int, v any) { w.WriteHeader(status); _ = json.NewEncoder(w).Encode(v) }
		order := func(state string) map[string]any {
			return map[string]any{"status": state, "identifiers": []map[string]string{{"type": "dns", "value": "public.example"}}, "authorizations": []string{endpoint + "/authz"}, "finalize": endpoint + "/finalize", "certificate": endpoint + "/cert"}
		}
		switch r.URL.Path {
		case "/directory":
			reply(200, map[string]any{"newNonce": endpoint + "/nonce", "newAccount": endpoint + "/account", "newOrder": endpoint + "/new-order"})
		case "/nonce":
			w.WriteHeader(200)
		case "/account":
			w.Header().Set("Location", endpoint+"/account/1")
			reply(201, map[string]any{"status": "valid", "orders": endpoint + "/orders"})
		case "/new-order":
			authorized = false
			w.Header().Set("Location", endpoint+"/order")
			reply(201, order("pending"))
		case "/authz":
			state := "pending"
			if authorized {
				state = "valid"
			}
			reply(200, map[string]any{"status": state, "identifier": map[string]string{"type": "dns", "value": "public.example"}, "challenges": []map[string]string{{"type": "http-01", "url": endpoint + "/challenge", "token": "test-token", "status": state}}})
		case "/challenge":
			response, err := http.Get("http://" + challengeAddress + "/.well-known/acme-challenge/test-token")
			if err != nil {
				t.Error(err)
				reply(400, map[string]string{"type": "urn:ietf:params:acme:error:connection", "detail": "challenge failed"})
				return
			}
			response.Body.Close()
			if response.StatusCode != 200 {
				t.Error("HTTP-01 failed", response.StatusCode)
			}
			authorized = true
			reply(200, map[string]string{"type": "http-01", "status": "valid", "url": endpoint + "/challenge", "token": "test-token"})
		case "/order":
			reply(200, order("ready"))
		case "/finalize":
			var jws struct {
				Payload string `json:"payload"`
			}
			if json.NewDecoder(r.Body).Decode(&jws) != nil {
				t.Error("invalid JWS")
				return
			}
			payload, _ := base64.RawURLEncoding.DecodeString(jws.Payload)
			var p struct {
				CSR string `json:"csr"`
			}
			if json.Unmarshal(payload, &p) != nil {
				t.Error("invalid payload")
				return
			}
			csrDER, _ := base64.RawURLEncoding.DecodeString(p.CSR)
			csr, err := x509.ParseCertificateRequest(csrDER)
			if err != nil || csr.CheckSignature() != nil {
				t.Error("invalid CSR", err)
				return
			}
			serial++
			host := "public.example"
			if wrongHost {
				host = "wrong.example"
			}
			cert := &x509.Certificate{SerialNumber: big.NewInt(serial), DNSNames: []string{host}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(6 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
			der, err := x509.CreateCertificate(rand.Reader, cert, ca, csr.PublicKey, caKey)
			if err != nil {
				t.Error(err)
				return
			}
			chain = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
			reply(200, order("valid"))
		case "/cert":
			w.Header().Set("Content-Type", "application/pem-certificate-chain")
			_, _ = w.Write(chain)
		default:
			t.Error("unexpected ACME path", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer mock.Close()
	endpoint = mock.URL
	v := GatewaySettings{Listen: "127.0.0.1:443", Host: "public.example", Managed: true, ChallengeListen: challengeAddress, ACMEDirectory: endpoint + "/directory"}
	c := &certificateCache{status: CertificateStatus{Mode: "managed", Host: v.Host}}
	certPath := filepath.Join(dir, "certificate.pem")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for i := int64(1); i <= 2; i++ {
		if err := renewManaged(ctx, dir, v, c, certPath); err != nil {
			t.Fatal("renewal", i, err)
		}
		pair, err := c.get(nil)
		if err != nil || pair.Leaf.SerialNumber.Int64() != i {
			t.Fatal("live certificate not switched", err)
		}
	}
	before, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	wrongHost = true
	mu.Unlock()
	if err := renewManaged(ctx, dir, v, c, certPath); err == nil {
		t.Fatal("wrong-host candidate accepted")
	}
	after, _ := os.ReadFile(certPath)
	if string(after) != string(before) {
		t.Fatal("bad candidate replaced durable certificate")
	}
	pair, err := c.get(nil)
	if err != nil || pair.Leaf.SerialNumber.Int64() != 2 {
		t.Fatal("bad candidate replaced live certificate")
	}
	// An unavailable validation listener must also leave the certificate intact.
	occupied, err := net.Listen("tcp", challengeAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	if err = renewManaged(ctx, dir, v, c, certPath); err == nil || !strings.Contains(err.Error(), "listener") {
		t.Fatal("expected bounded listener failure", err)
	}
}
