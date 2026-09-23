// Package config loads the local, non-authoritative process configuration.
// Cluster membership and routing facts remain in the Root Group.
package config

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"gopkg.in/yaml.v3"
)

const (
	FormatVersion       = 1
	maxConfigBytes      = 1 << 20
	maxCertificateBytes = 1 << 20
)

type TLSFiles struct {
	Certificate string `yaml:"certificate"`
	PrivateKey  string `yaml:"private_key"`
	CA          string `yaml:"ca"`
	ServerName  string `yaml:"server_name"`
}

type BootstrapPeer struct {
	RaftID  uint64 `yaml:"raft_id"`
	Address string `yaml:"address"`
}

type Config struct {
	Version            int             `yaml:"version"`
	StoreRoot          string          `yaml:"store_root"`
	Listen             string          `yaml:"listen"`
	Advertise          string          `yaml:"advertise"`
	TLS                TLSFiles        `yaml:"tls"`
	RootBootstrap      []BootstrapPeer `yaml:"root_bootstrap"`
	TickInterval       time.Duration   `yaml:"-"`
	MembershipInterval time.Duration   `yaml:"-"`
	MaxMessageSize     int             `yaml:"max_message_size"`
}

type fileConfig struct {
	Version            int             `yaml:"version"`
	StoreRoot          string          `yaml:"store_root"`
	Listen             string          `yaml:"listen"`
	Advertise          string          `yaml:"advertise"`
	TLS                TLSFiles        `yaml:"tls"`
	RootBootstrap      []BootstrapPeer `yaml:"root_bootstrap"`
	TickInterval       string          `yaml:"tick_interval"`
	MembershipInterval string          `yaml:"membership_interval"`
	MaxMessageSize     int             `yaml:"max_message_size"`
}

func Load(path string) (Config, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return Config{}, errors.New("configuration path is required")
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read configuration: %w", err)
	}
	if len(encoded) == 0 || len(encoded) > maxConfigBytes {
		return Config{}, errors.New("configuration is empty or exceeds 1 MiB")
	}
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	decoder.KnownFields(true)
	var raw fileConfig
	if err := decoder.Decode(&raw); err != nil {
		return Config{}, fmt.Errorf("decode configuration: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Config{}, errors.New("configuration contains multiple YAML documents")
	}
	base, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return Config{}, err
	}
	result := Config{Version: raw.Version, StoreRoot: resolvePath(base, raw.StoreRoot),
		Listen: strings.TrimSpace(raw.Listen), Advertise: strings.TrimSpace(raw.Advertise),
		TLS: TLSFiles{Certificate: resolvePath(base, raw.TLS.Certificate),
			PrivateKey: resolvePath(base, raw.TLS.PrivateKey), CA: resolvePath(base, raw.TLS.CA),
			ServerName: strings.TrimSpace(raw.TLS.ServerName)},
		RootBootstrap: raw.RootBootstrap, MaxMessageSize: raw.MaxMessageSize}
	defaultPKI := nodepki.StorePaths(result.StoreRoot)
	if result.TLS.Certificate == "" {
		result.TLS.Certificate = defaultPKI.Certificate
	}
	if result.TLS.PrivateKey == "" {
		result.TLS.PrivateKey = defaultPKI.PrivateKey
	}
	if result.TLS.CA == "" {
		result.TLS.CA = defaultPKI.CACertificate
	}
	if result.TLS.ServerName == "" {
		result.TLS.ServerName = nodepki.DefaultServerName
	}
	if result.TickInterval, err = parseDuration(raw.TickInterval, 100*time.Millisecond); err != nil {
		return Config{}, fmt.Errorf("tick_interval: %w", err)
	}
	if result.MembershipInterval, err = parseDuration(raw.MembershipInterval, time.Second); err != nil {
		return Config{}, fmt.Errorf("membership_interval: %w", err)
	}
	if result.MaxMessageSize == 0 {
		result.MaxMessageSize = raftrpc.DefaultMaxMessageSize
	}
	if err := result.Validate(); err != nil {
		return Config{}, err
	}
	return result, nil
}

func (config Config) Validate() error {
	if config.Version != FormatVersion || config.StoreRoot == "" ||
		config.TLS.Certificate == "" || config.TLS.PrivateKey == "" || config.TLS.CA == "" ||
		nodepki.ValidateServerName(config.TLS.ServerName) != nil {
		return errors.New("configuration requires version 1, StoreRoot, certificate, private key, CA, and a valid TLS server name")
	}
	if validateAddress(config.Listen) != nil || validateAddress(config.Advertise) != nil {
		return errors.New("listen and advertise must be valid host:port addresses")
	}
	if config.TickInterval <= 0 || config.MembershipInterval <= 0 ||
		config.MaxMessageSize <= 0 || config.MaxMessageSize > 64<<20 {
		return errors.New("configuration intervals and message size are outside supported bounds")
	}
	seen := make(map[uint64]bool, len(config.RootBootstrap))
	for index := range config.RootBootstrap {
		peer := &config.RootBootstrap[index]
		peer.Address = strings.TrimSpace(peer.Address)
		if peer.RaftID == 0 || seen[peer.RaftID] || validateAddress(peer.Address) != nil {
			return errors.New("Root bootstrap peers require unique non-zero RaftIDs and valid addresses")
		}
		seen[peer.RaftID] = true
	}
	return nil
}

func (config Config) RootPeers() []uint64 {
	result := make([]uint64, 0, len(config.RootBootstrap))
	for _, peer := range config.RootBootstrap {
		result = append(result, peer.RaftID)
	}
	return result
}

func (config Config) BootstrapDirectory() (*raftrpc.StaticDirectory, error) {
	directory := raftrpc.NewStaticDirectory()
	for _, peer := range config.RootBootstrap {
		if err := directory.Update(peer.RaftID, peer.Address); err != nil {
			return nil, err
		}
	}
	return directory, nil
}

func (config Config) LoadTLS(value identity.Identity) (*tls.Config, *tls.Config, error) {
	if err := privateFile(config.TLS.PrivateKey); err != nil {
		return nil, nil, err
	}
	certificate, err := tls.LoadX509KeyPair(config.TLS.Certificate, config.TLS.PrivateKey)
	if err != nil {
		return nil, nil, fmt.Errorf("load Node certificate: %w", err)
	}
	if len(certificate.Certificate) == 0 {
		return nil, nil, errors.New("Node certificate chain is empty")
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return nil, nil, fmt.Errorf("parse Node certificate: %w", err)
	}
	if err := identity.ValidateNodeCertificate(leaf, value); err != nil {
		return nil, nil, err
	}
	certificate.Leaf = leaf
	pool, err := loadCAPool(config.TLS.CA)
	if err != nil {
		return nil, nil, err
	}
	intermediates := x509.NewCertPool()
	for _, encoded := range certificate.Certificate[1:] {
		parsed, err := x509.ParseCertificate(encoded)
		if err != nil {
			return nil, nil, errors.New("Node certificate chain contains an invalid certificate")
		}
		intermediates.AddCert(parsed)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool, Intermediates: intermediates,
		DNSName: config.TLS.ServerName, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
		return nil, nil, fmt.Errorf("verify Node server certificate: %w", err)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool, Intermediates: intermediates,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return nil, nil, fmt.Errorf("verify Node client certificate: %w", err)
	}
	serverTLS := &tls.Config{Certificates: []tls.Certificate{certificate}, ClientCAs: pool,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13}
	clientTLS := &tls.Config{Certificates: []tls.Certificate{certificate}, RootCAs: pool,
		ServerName: config.TLS.ServerName, MinVersion: tls.VersionTLS13}
	return serverTLS, clientTLS, nil
}

func loadCAPool(path string) (*x509.CertPool, error) {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read Node CA: %w", err)
	}
	if len(encoded) == 0 || len(encoded) > maxCertificateBytes {
		return nil, errors.New("Node CA is empty or exceeds 1 MiB")
	}
	pool := x509.NewCertPool()
	var certificates []*x509.Certificate
	for len(bytes.TrimSpace(encoded)) != 0 {
		block, rest := pem.Decode(encoded)
		if block == nil || block.Type != "CERTIFICATE" {
			return nil, errors.New("Node CA contains invalid PEM data")
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !certificate.IsCA {
			return nil, errors.New("Node CA contains an invalid CA certificate")
		}
		pool.AddCert(certificate)
		certificates = append(certificates, certificate)
		encoded = rest
	}
	if len(certificates) == 0 {
		return nil, errors.New("Node CA has no certificates")
	}
	return pool, nil
}

func resolvePath(base, value string) string {
	value = strings.TrimSpace(value)
	if value == "" || filepath.IsAbs(value) {
		return value
	}
	return filepath.Join(base, value)
}

func parseDuration(value string, fallback time.Duration) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, errors.New("duration must be positive")
	}
	return parsed, nil
}

func validateAddress(address string) error {
	host, port, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil || strings.TrimSpace(host) == "" {
		return errors.New("invalid address")
	}
	value, err := strconv.ParseUint(port, 10, 16)
	if err != nil || value == 0 {
		return errors.New("invalid port")
	}
	return nil
}

func privateFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("inspect Node private key: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return errors.New("Node private key must be a regular file inaccessible to group and others")
	}
	return nil
}
