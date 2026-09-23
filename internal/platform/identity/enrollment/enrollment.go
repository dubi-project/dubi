// Package enrollment owns the leaf-only side of DUBI Node enrollment. It
// prepares an unbound durable identity and CSR, then installs a Cluster-issued
// bundle without ever receiving the Node CA signer.
package enrollment

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
)

const (
	FormatVersion  = 1
	DirectoryName  = "dubi-node-enrollment"
	RequestName    = "request.json"
	PrivateKeyName = "node.key"
	maxDocument    = 1 << 20
)

var regionID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

type Request struct {
	Version    int    `json:"version"`
	NodeID     string `json:"node_id"`
	RaftID     uint64 `json:"raft_id"`
	ServerName string `json:"server_name"`
	RegionID   string `json:"region_id"`
	Address    string `json:"address"`
	CSRPEM     string `json:"csr_pem"`
}

type Bundle struct {
	Version          int    `json:"version"`
	ClusterID        string `json:"cluster_id"`
	NodeID           string `json:"node_id"`
	RaftID           uint64 `json:"raft_id"`
	ServerName       string `json:"server_name"`
	CACertificatePEM string `json:"ca_certificate_pem"`
	CertificatePEM   string `json:"certificate_pem"`
}

type Paths struct {
	Directory  string
	Request    string
	PrivateKey string
}

func StorePaths(store string) Paths {
	directory := filepath.Join(store, DirectoryName)
	return Paths{Directory: directory, Request: filepath.Join(directory, RequestName),
		PrivateKey: filepath.Join(directory, PrivateKeyName)}
}

// Prepare creates a stable unbound identity and CSR, or verifies and returns
// the existing request. Retrying with different metadata is rejected.
func Prepare(store, serverName, region, address string) (identity.Identity, Request, error) {
	serverName, region, address = strings.TrimSpace(serverName), strings.TrimSpace(region), strings.TrimSpace(address)
	if err := validateMetadata(serverName, region, address); err != nil {
		return identity.Identity{}, Request{}, err
	}
	value, err := identity.PrepareJoin(store)
	if err != nil {
		return identity.Identity{}, Request{}, err
	}
	if value.ClusterID != "" {
		return identity.Identity{}, Request{}, errors.New("Node identity is already bound to a Cluster")
	}
	paths := StorePaths(store)
	if _, err := os.Lstat(paths.Directory); err == nil {
		request, _, loadErr := loadPrepared(store, value)
		if loadErr != nil {
			return identity.Identity{}, Request{}, loadErr
		}
		if request.ServerName != serverName || request.RegionID != region || request.Address != address {
			return identity.Identity{}, Request{}, errors.New("existing enrollment request uses different Node metadata")
		}
		return value, request, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return identity.Identity{}, Request{}, err
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return identity.Identity{}, Request{}, err
	}
	joinURI, err := identity.JoinIdentityURI(value)
	if err != nil {
		return identity.Identity{}, Request{}, err
	}
	template := &x509.CertificateRequest{Subject: pkix.Name{CommonName: value.NodeID}, URIs: []*url.URL{joinURI}}
	if address := net.ParseIP(serverName); address != nil {
		template.IPAddresses = []net.IP{address}
	} else {
		template.DNSNames = []string{serverName}
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, template, privateKey)
	if err != nil {
		return identity.Identity{}, Request{}, err
	}
	request := Request{Version: FormatVersion, NodeID: value.NodeID, RaftID: value.RaftID,
		ServerName: serverName, RegionID: region, Address: address,
		CSRPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER}))}
	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return identity.Identity{}, Request{}, err
	}
	privatePEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER})
	if err := publishPrepared(store, request, privatePEM); err != nil {
		return identity.Identity{}, Request{}, err
	}
	loaded, _, err := loadPrepared(store, value)
	return value, loaded, err
}

func EncodeRequest(request Request) ([]byte, error) {
	if _, err := validateRequest(request); err != nil {
		return nil, err
	}
	return json.MarshalIndent(request, "", "  ")
}

func DecodeRequest(encoded []byte) (Request, error) {
	var request Request
	if err := decodeStrict(encoded, &request); err != nil {
		return Request{}, err
	}
	if _, err := validateRequest(request); err != nil {
		return Request{}, err
	}
	return request, nil
}

func EncodeBundle(bundle Bundle) ([]byte, error) {
	if err := validateBundle(bundle); err != nil {
		return nil, err
	}
	return json.MarshalIndent(bundle, "", "  ")
}

func DecodeBundle(encoded []byte) (Bundle, error) {
	var bundle Bundle
	if err := decodeStrict(encoded, &bundle); err != nil {
		return Bundle{}, err
	}
	if err := validateBundle(bundle); err != nil {
		return Bundle{}, err
	}
	return bundle, nil
}

// Install verifies that the issued bundle matches the persisted CSR and
// private key before binding the durable identity to its Cluster.
func Install(store string, bundle Bundle, now time.Time) (nodepki.Material, error) {
	if err := validateBundle(bundle); err != nil {
		return nodepki.Material{}, err
	}
	value, err := identity.Load(store)
	if err != nil {
		return nodepki.Material{}, err
	}
	if value.NodeID != bundle.NodeID || value.RaftID != bundle.RaftID ||
		(value.ClusterID != "" && value.ClusterID != bundle.ClusterID) {
		return nodepki.Material{}, errors.New("enrollment bundle does not match durable Node identity")
	}
	candidate := value
	candidate.ClusterID = bundle.ClusterID
	if err := nodepki.ValidateIssuedLeaf(candidate, bundle.ServerName,
		[]byte(bundle.CACertificatePEM), []byte(bundle.CertificatePEM), now); err != nil {
		return nodepki.Material{}, fmt.Errorf("validate enrollment bundle: %w", err)
	}
	if value.ClusterID != "" {
		material, loadErr := nodepki.Load(store, value, bundle.ServerName, now)
		if loadErr == nil {
			_ = removePrepared(store)
			return material, nil
		}
	}
	request, privatePEM, err := loadPrepared(store, value)
	if err != nil {
		return nodepki.Material{}, err
	}
	if request.NodeID != bundle.NodeID || request.RaftID != bundle.RaftID || request.ServerName != bundle.ServerName {
		return nodepki.Material{}, errors.New("enrollment bundle does not match persisted request")
	}
	material, err := nodepki.InstallLeaf(store, value, bundle.ClusterID, bundle.ServerName,
		[]byte(bundle.CACertificatePEM), []byte(bundle.CertificatePEM), privatePEM, now)
	if err != nil {
		return nodepki.Material{}, err
	}
	if err := removePrepared(store); err != nil {
		return nodepki.Material{}, errors.Join(err, errors.New("remove completed enrollment request"))
	}
	return material, nil
}

func loadPrepared(store string, value identity.Identity) (Request, []byte, error) {
	paths := StorePaths(store)
	if err := validateDirectory(paths.Directory); err != nil {
		return Request{}, nil, err
	}
	encoded, err := readFile(paths.Request, 0o600)
	if err != nil {
		return Request{}, nil, err
	}
	request, err := DecodeRequest(encoded)
	if err != nil {
		return Request{}, nil, err
	}
	if request.NodeID != value.NodeID || request.RaftID != value.RaftID {
		return Request{}, nil, errors.New("enrollment request does not match durable Node identity")
	}
	privatePEM, err := readFile(paths.PrivateKey, 0o600)
	if err != nil {
		return Request{}, nil, err
	}
	privateKey, err := parsePrivateKey(privatePEM)
	if err != nil {
		return Request{}, nil, err
	}
	csr, err := validateRequest(request)
	if err != nil {
		return Request{}, nil, err
	}
	publicKey, ok := csr.PublicKey.(ed25519.PublicKey)
	if !ok || !bytes.Equal(publicKey, privateKey.Public().(ed25519.PublicKey)) {
		return Request{}, nil, errors.New("enrollment private key does not match CSR")
	}
	return request, privatePEM, nil
}

func publishPrepared(store string, request Request, privatePEM []byte) error {
	paths := StorePaths(store)
	staging, err := os.MkdirTemp(store, ".dubi-node-enrollment-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(staging)
	if err := os.Chmod(staging, 0o700); err != nil {
		return err
	}
	encoded, err := EncodeRequest(request)
	if err != nil {
		return err
	}
	stagingPaths := pathsAt(staging)
	if err := writeExclusive(stagingPaths.Request, append(encoded, '\n'), 0o600); err != nil {
		return err
	}
	if err := writeExclusive(stagingPaths.PrivateKey, privatePEM, 0o600); err != nil {
		return err
	}
	if err := syncDirectory(staging); err != nil {
		return err
	}
	if err := os.Rename(staging, paths.Directory); err != nil {
		return fmt.Errorf("publish enrollment request: %w", err)
	}
	return syncDirectory(store)
}

func validateRequest(request Request) (*x509.CertificateRequest, error) {
	if request.Version != FormatVersion || request.NodeID == "" || request.RaftID == 0 ||
		validateMetadata(request.ServerName, request.RegionID, request.Address) != nil {
		return nil, errors.New("invalid Node enrollment request metadata")
	}
	block, rest := pem.Decode([]byte(request.CSRPEM))
	if block == nil || block.Type != "CERTIFICATE REQUEST" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("Node enrollment request must contain one PEM CSR")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil || csr.CheckSignature() != nil {
		return nil, errors.New("Node enrollment CSR signature is invalid")
	}
	value := identity.Identity{Version: 1, NodeID: request.NodeID, RaftID: request.RaftID, CreatedAt: time.Unix(1, 0).UTC()}
	joinURI, err := identity.JoinIdentityURI(value)
	if err != nil || csr.Subject.CommonName != request.NodeID || len(csr.URIs) != 1 || csr.URIs[0].String() != joinURI.String() {
		return nil, errors.New("Node enrollment CSR identity is invalid")
	}
	if address := net.ParseIP(request.ServerName); address != nil {
		if len(csr.IPAddresses) != 1 || !csr.IPAddresses[0].Equal(address) || len(csr.DNSNames) != 0 {
			return nil, errors.New("Node enrollment CSR server IP is invalid")
		}
	} else if len(csr.DNSNames) != 1 || csr.DNSNames[0] != request.ServerName || len(csr.IPAddresses) != 0 {
		return nil, errors.New("Node enrollment CSR server DNS name is invalid")
	}
	if _, ok := csr.PublicKey.(ed25519.PublicKey); !ok {
		return nil, errors.New("Node enrollment CSR must use Ed25519")
	}
	return csr, nil
}

func validateBundle(bundle Bundle) error {
	if bundle.Version != FormatVersion || bundle.ClusterID == "" || bundle.NodeID == "" || bundle.RaftID == 0 ||
		nodepki.ValidateServerName(bundle.ServerName) != nil || bundle.CACertificatePEM == "" || bundle.CertificatePEM == "" {
		return errors.New("invalid Node enrollment bundle")
	}
	return nil
}

func validateMetadata(serverName, region, address string) error {
	if nodepki.ValidateServerName(serverName) != nil || !regionID.MatchString(region) {
		return errors.New("enrollment requires a valid server name and RegionID")
	}
	host, port, err := net.SplitHostPort(address)
	parsedPort, portErr := strconv.ParseUint(port, 10, 16)
	if err != nil || strings.TrimSpace(host) == "" || portErr != nil || parsedPort == 0 {
		return errors.New("enrollment advertise address must be host:port")
	}
	return nil
}

func decodeStrict(encoded []byte, target any) error {
	if len(encoded) == 0 || len(encoded) > maxDocument {
		return errors.New("enrollment document is empty or exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil || decoder.Decode(new(any)) != io.EOF {
		return errors.New("invalid enrollment JSON document")
	}
	return nil
}

func parsePrivateKey(encoded []byte) (ed25519.PrivateKey, error) {
	block, rest := pem.Decode(encoded)
	if block == nil || block.Type != "PRIVATE KEY" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("invalid enrollment private key PEM")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	privateKey, ok := parsed.(ed25519.PrivateKey)
	if err != nil || !ok {
		return nil, errors.New("enrollment private key must use Ed25519")
	}
	return privateKey, nil
}

func pathsAt(directory string) Paths {
	return Paths{Directory: directory, Request: filepath.Join(directory, RequestName),
		PrivateKey: filepath.Join(directory, PrivateKeyName)}
}

func validateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return errors.Join(err, errors.New("enrollment directory must be a real 0700 directory"))
	}
	return nil
}

func readFile(path string, mode os.FileMode) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || info == nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != mode {
		return nil, errors.Join(err, fmt.Errorf("enrollment file %s must be a regular %04o file", filepath.Base(path), mode))
	}
	encoded, err := os.ReadFile(path)
	if err != nil || len(encoded) == 0 || len(encoded) > maxDocument {
		return nil, errors.Join(err, errors.New("enrollment file is empty or too large"))
	}
	return encoded, nil
}

func writeExclusive(path string, data []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	return errors.Join(err, file.Close())
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func removePrepared(store string) error {
	paths := StorePaths(store)
	if err := os.RemoveAll(paths.Directory); err != nil {
		return err
	}
	return syncDirectory(store)
}
