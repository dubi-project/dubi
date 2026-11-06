package config

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/crypto/acme"
	"golang.org/x/sys/unix"
)

const productionDirectory = "https://acme-v02.api.letsencrypt.org/directory"

func privateWrite(path string, b []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".new-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
func prepareManagedTLS(store string, v GatewaySettings) (*GatewayTLS, error) {
	if strings.ContainsAny(v.Host, "/:\\ ") && net.ParseIP(v.Host) == nil {
		return nil, errors.New("api host must be a hostname or IP without scheme or port")
	}
	if ip := net.ParseIP(v.Host); ip != nil && (!ip.IsGlobalUnicast() || ip.IsPrivate()) {
		return nil, errors.New("managed public certificates require a public IP or hostname")
	}
	directory := filepath.Join(store, "gateway-pki")
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	c := &certificateCache{status: CertificateStatus{Mode: "managed", Host: v.Host}}
	certificatePath := filepath.Join(directory, "certificate.pem")
	if b, err := os.ReadFile(certificatePath); err == nil {
		if pair, err := tls.X509KeyPair(b, b); err == nil {
			_ = c.candidate(&pair, time.Now())
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	saveStatus := func() {
		b, err := json.Marshal(c.view())
		if err == nil {
			_ = privateWrite(filepath.Join(directory, "status.json"), b)
		}
	}
	run := func(ctx context.Context) {
		delay := time.Duration(0)
		backoff := time.Minute
		for {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			c.mu.Lock()
			pair := c.pair
			due := pair == nil || time.Until(pair.Leaf.NotAfter) < pair.Leaf.NotAfter.Sub(pair.Leaf.NotBefore)/3
			c.mu.Unlock()
			if !due {
				delay = time.Hour
				c.mu.Lock()
				c.status.NextAttemptUnix = time.Now().Add(delay).Unix()
				c.mu.Unlock()
				saveStatus()
				continue
			}
			call, cancel := context.WithTimeout(ctx, 3*time.Minute)
			err := renewManaged(call, directory, v, c, certificatePath)
			cancel()
			c.mu.Lock()
			if err != nil {
				c.status.LastError = "ACME renewal failed: " + err.Error()
				delay = backoff
				backoff = min(backoff*2, time.Hour)
			} else {
				c.status.LastError = ""
				delay = time.Hour
				backoff = time.Minute
			}
			c.status.NextAttemptUnix = time.Now().Add(delay).Unix()
			c.mu.Unlock()
			saveStatus()
		}
	}
	return &GatewayTLS{TLS: &tls.Config{MinVersion: tls.VersionTLS13, GetCertificate: c.get}, Run: run, Status: c.view}, nil
}
func renewManaged(ctx context.Context, dir string, v GatewaySettings, c *certificateCache, path string) error {
	lock, err := os.OpenFile(filepath.Join(dir, "renew.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return errors.New("cannot open renewal lock")
	}
	defer lock.Close()
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return errors.New("another process is renewing")
	}
	defer unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	keyPath := filepath.Join(dir, "account.pem")
	var account *ecdsa.PrivateKey
	b, err := os.ReadFile(keyPath)
	if errors.Is(err, os.ErrNotExist) {
		account, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return err
		}
		der, err := x509.MarshalECPrivateKey(account)
		if err != nil {
			return err
		}
		if err = privateWrite(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der})); err != nil {
			return err
		}
	} else if err != nil {
		return errors.New("cannot read ACME account")
	} else {
		block, _ := pem.Decode(b)
		if block == nil {
			return errors.New("invalid ACME account")
		}
		account, err = x509.ParseECPrivateKey(block.Bytes)
		if err != nil {
			return errors.New("invalid ACME account key")
		}
	}
	address := v.ChallengeListen
	if address == "" {
		address = "0.0.0.0:80"
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return errors.New("HTTP-01 validation listener unavailable; port 80 must be reachable")
	}
	mux := http.NewServeMux()
	validation := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 10 * time.Second, MaxHeaderBytes: 8192}
	go func() { _ = validation.Serve(listener) }()
	defer validation.Close()
	endpoint := v.ACMEDirectory
	if endpoint == "" {
		endpoint = productionDirectory
	}
	client := &acme.Client{Key: account, DirectoryURL: endpoint, UserAgent: "dubi-gateway", HTTPClient: &http.Client{Timeout: 30 * time.Second}}
	_, err = client.Register(ctx, &acme.Account{}, acme.AcceptTOS)
	if err != nil && !errors.Is(err, acme.ErrAccountAlreadyExists) {
		return errors.New("ACME account registration failed")
	}
	ids := acme.DomainIDs(v.Host)
	var opts []acme.OrderOption
	if net.ParseIP(v.Host) != nil {
		ids = acme.IPIDs(v.Host)
		opts = append(opts, acme.WithOrderIssuanceProfile("shortlived"))
	}
	order, err := client.AuthorizeOrder(ctx, ids, opts...)
	if err != nil {
		return fmt.Errorf("create certificate order: %w", err)
	}
	for _, uri := range order.AuthzURLs {
		a, err := client.GetAuthorization(ctx, uri)
		if err != nil {
			return errors.New("read ACME authorization failed")
		}
		if a.Status == acme.StatusValid {
			continue
		}
		var challenge *acme.Challenge
		for _, ch := range a.Challenges {
			if ch.Type == "http-01" {
				challenge = ch
				break
			}
		}
		if challenge == nil {
			return errors.New("CA does not offer HTTP-01")
		}
		response, err := client.HTTP01ChallengeResponse(challenge.Token)
		if err != nil {
			return err
		}
		mux.HandleFunc("GET "+client.HTTP01ChallengePath(challenge.Token), func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte(response))
		})
		if _, err = client.Accept(ctx, challenge); err != nil {
			return errors.New("ACME challenge submission failed")
		}
		if _, err = client.WaitAuthorization(ctx, uri); err != nil {
			return errors.New("address validation failed; check public port 80 and DNS/IP routing")
		}
	}
	order, err = client.WaitOrder(ctx, order.URI)
	if err != nil {
		return errors.New("ACME order did not become ready")
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	template := &x509.CertificateRequest{}
	if ip := net.ParseIP(v.Host); ip != nil {
		template.IPAddresses = []net.IP{ip}
	} else {
		template.DNSNames = []string{v.Host}
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, template, leafKey)
	if err != nil {
		return err
	}
	chain, _, err := client.CreateCertFromOrder(ctx, order, csr, true)
	if err != nil {
		return errors.New("ACME certificate finalization failed")
	}
	var material []byte
	for _, der := range chain {
		material = append(material, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}
	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		return err
	}
	material = append(material, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})...)
	pair, err := tls.X509KeyPair(material, material)
	if err != nil {
		return err
	}
	// Validate before replacing disk material or the live certificate.
	candidate := &certificateCache{status: CertificateStatus{Host: v.Host}}
	if err = candidate.candidate(&pair, time.Now()); err != nil {
		return err
	}
	if err = privateWrite(path, material); err != nil {
		return errors.New("cannot persist renewed certificate")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.candidate(&pair, time.Now())
}
