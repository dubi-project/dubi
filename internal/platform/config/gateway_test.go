package config

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGatewaySettingsPersistenceAndCorruption(t *testing.T) {
	root := t.TempDir()
	v := GatewaySettings{Listen: "127.0.0.1:7444", Host: "public.example", Managed: true}
	if err := SaveGateway(root, v); err != nil {
		t.Fatal(err)
	}
	got, err := LoadGateway(root)
	if err != nil || got.Host != v.Host || !got.Managed {
		t.Fatal(got, err)
	}
	if err := os.WriteFile(filepath.Join(root, "gateway.json"), []byte(`{"version":1,"unknown":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadGateway(root); err == nil {
		t.Fatal("accepted malformed saved settings")
	}
}
func TestGatewayExternalCertificateKeepsValidCandidate(t *testing.T) {
	root := t.TempDir()
	cert, key := filepath.Join(root, "cert.pem"), filepath.Join(root, "key.pem")
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	der, _ := x509.MarshalECPrivateKey(k)
	if err := os.WriteFile(key, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	issue := func(serial int64) {
		c := &x509.Certificate{SerialNumber: big.NewInt(serial), DNSNames: []string{"public.example"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
		der, err := x509.CreateCertificate(rand.Reader, c, c, &k.PublicKey, k)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
			t.Fatal(err)
		}
	}
	issue(1)
	g, err := PrepareGatewayTLS(root, GatewaySettings{Listen: "127.0.0.1:443", Host: "public.example", Certificate: cert, PrivateKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(key, []byte("broken candidate"), 0600); err != nil {
		t.Fatal(err)
	}
	pair, err := g.TLS.GetCertificate(nil)
	if err != nil || pair.Leaf.SerialNumber.Int64() != 1 {
		t.Fatal("lost valid cached cert", err)
	}
	if g.Status().(CertificateStatus).LastError == "" {
		t.Fatal("failed reload not visible")
	}
	if err = os.WriteFile(key, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	issue(2)
	time.Sleep(1100 * time.Millisecond)
	pair, err = g.TLS.GetCertificate(nil)
	if err != nil || pair.Leaf.SerialNumber.Int64() != 2 {
		t.Fatal("valid renewed certificate not loaded", err)
	}
	if g.Status().(CertificateStatus).LastError != "" {
		t.Fatal("renewal failure was not cleared")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g.Run(ctx)
	if _, err := PrepareGatewayTLS(root, GatewaySettings{Listen: "127.0.0.1:443", Host: "wrong.example", Certificate: cert, PrivateKey: key}); err == nil {
		t.Fatal("bad startup certificate accepted")
	}
}
