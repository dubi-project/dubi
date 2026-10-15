// Package config supplies local process defaults and managed Node TLS.
// Identity, consensus membership and PKI names are never user configuration.
package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
)

const DefaultListen = "0.0.0.0:7443"

func DefaultStore() string {
	if value := strings.TrimSpace(os.Getenv("DUBI_STORE")); value != "" {
		return value
	}
	return "/var/lib/dubi"
}

// ValidateListen also accepts port zero, allowing the OS to select a port.
func ValidateListen(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || strings.TrimSpace(host) == "" {
		return errors.New("listen must be a host:port address")
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return errors.New("listen port must be between 0 and 65535")
	}
	return nil
}

// LoadTLS only uses identity-bound material managed by init/enrollment.
// There are no certificate path, CA or internal server-name overrides.
func LoadTLS(store string, value identity.Identity) (*tls.Config, *tls.Config, error) {
	material, certificate, err := nodepki.LoadTLSCertificate(store, value, nodepki.DefaultServerName, time.Now().UTC())
	if err != nil {
		return nil, nil, fmt.Errorf("load managed Node PKI: %w", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(material.CACertificate)
	server := &tls.Config{Certificates: []tls.Certificate{certificate}, ClientCAs: pool,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13}
	client := &tls.Config{Certificates: []tls.Certificate{certificate}, RootCAs: pool,
		ServerName: nodepki.DefaultServerName, MinVersion: tls.VersionTLS13}
	return server, client, nil
}

// LoadReloadableTLS is the long-running server variant. One-off clients keep
// using LoadTLS; the server and its outgoing transports share a leaf reloader.
// Leave Certificates empty so even a ClientHello without SNI uses the callback.
func LoadReloadableTLS(store string, value identity.Identity) (*tls.Config, *tls.Config, error) {
	r, err := nodepki.NewLeafReloader(store, value, nodepki.DefaultServerName)
	if err != nil {
		return nil, nil, fmt.Errorf("load reloadable managed Node PKI: %w", err)
	}
	server := &tls.Config{GetCertificate: r.GetCertificate, ClientCAs: r.TrustPool(),
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13}
	client := &tls.Config{GetClientCertificate: r.GetClientCertificate, RootCAs: r.TrustPool(),
		ServerName: nodepki.DefaultServerName, MinVersion: tls.VersionTLS13}
	return server, client, nil
}
