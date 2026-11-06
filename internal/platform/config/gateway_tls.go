package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

type GatewayTLS struct {
	TLS    *tls.Config
	Run    func(context.Context)
	Status func() any
}
type CertificateStatus struct {
	Mode            string `json:"mode"`
	Host            string `json:"host,omitempty"`
	ExpiresUnix     int64  `json:"expires_unix,omitempty"`
	LastSuccessUnix int64  `json:"last_success_unix,omitempty"`
	LastError       string `json:"last_error,omitempty"`
	NextAttemptUnix int64  `json:"next_attempt_unix,omitempty"`
}
type certificateCache struct {
	mu        sync.Mutex
	pair      *tls.Certificate
	status    CertificateStatus
	load      func() (*tls.Certificate, error)
	lastCheck time.Time
}

func (s CertificateStatus) GatewayMetrics() string {
	failed := 0
	if s.LastError != "" {
		failed = 1
	}
	return fmt.Sprintf("# TYPE dubi_gateway_certificate_expires_unix gauge\ndubi_gateway_certificate_expires_unix %d\n# TYPE dubi_gateway_certificate_update_failed gauge\ndubi_gateway_certificate_update_failed %d\n# TYPE dubi_gateway_certificate_next_attempt_unix gauge\ndubi_gateway_certificate_next_attempt_unix %d\n", s.ExpiresUnix, failed, s.NextAttemptUnix)
}
func (c *certificateCache) candidate(pair *tls.Certificate, now time.Time) error {
	if pair == nil || len(pair.Certificate) == 0 {
		return errors.New("empty public certificate")
	}

	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return err
	}
	if now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
		return errors.New("public certificate is not currently valid")
	}
	if leaf.KeyUsage != 0 && leaf.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
		return errors.New("certificate cannot sign a TLS handshake")
	}
	if len(leaf.ExtKeyUsage) > 0 {
		valid := false
		for _, usage := range leaf.ExtKeyUsage {
			if usage == x509.ExtKeyUsageServerAuth || usage == x509.ExtKeyUsageAny {
				valid = true
			}
		}
		if !valid {
			return errors.New("certificate is not valid for server authentication")
		}
	}
	if c.status.Host != "" {
		if err = leaf.VerifyHostname(c.status.Host); err != nil {
			return errors.New("public certificate does not cover configured host")
		}
	}
	pair.Leaf = leaf
	c.pair = pair
	c.status.ExpiresUnix = leaf.NotAfter.Unix()
	c.status.LastSuccessUnix = now.Unix()
	c.status.LastError = ""
	return nil
}
func (c *certificateCache) get(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if c.load != nil && (c.lastCheck.IsZero() || now.Sub(c.lastCheck) > time.Second) {
		c.lastCheck = now
		candidate, err := c.load()
		if err == nil {
			err = c.candidate(candidate, now)
		}
		if err != nil {
			c.status.LastError = "certificate update rejected; check certificate files, address and validity"
		}
	}
	if c.pair == nil || now.Before(c.pair.Leaf.NotBefore) || !now.Before(c.pair.Leaf.NotAfter) {
		return nil, errors.New("no valid public certificate available")
	}
	return c.pair, nil
}
func (c *certificateCache) view() any { c.mu.Lock(); defer c.mu.Unlock(); return c.status }
func PrepareGatewayTLS(store string, v GatewaySettings) (*GatewayTLS, error) {
	if v.Listen == "" {
		return &GatewayTLS{}, nil
	}

	if v.Managed {
		if v.Listen == "" || v.Host == "" || v.Certificate != "" || v.PrivateKey != "" {
			return nil, errors.New("--api-acme requires --api-listen and --api-host, without external certificate paths")
		}
		return prepareManagedTLS(store, v)
	}
	if v.Certificate == "" && v.PrivateKey == "" {
		return &GatewayTLS{Status: func() any {
			return CertificateStatus{Mode: "internal", LastError: "private Node CA; configure a public certificate for browser access"}
		}}, nil
	}
	if v.Listen == "" || v.Certificate == "" || v.PrivateKey == "" {
		return nil, errors.New("public certificate requires listener, chain and private key")
	}
	c := &certificateCache{status: CertificateStatus{Mode: "external", Host: v.Host}}
	c.load = func() (*tls.Certificate, error) {
		info, err := os.Stat(v.PrivateKey)
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			return nil, errors.New("public private key must be a private regular file")
		}
		p, err := tls.LoadX509KeyPair(v.Certificate, v.PrivateKey)
		return &p, err
	}
	pair, err := c.load()
	if err != nil {
		return nil, fmt.Errorf("load public certificate: %w", err)
	}
	if err = c.candidate(pair, time.Now()); err != nil {
		return nil, err
	}
	return &GatewayTLS{TLS: &tls.Config{MinVersion: tls.VersionTLS13, GetCertificate: c.get}, Status: c.view, Run: func(ctx context.Context) {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = c.get(nil)
			}
		}
	}}, nil
}
