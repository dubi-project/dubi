// Package repository defines the engine-neutral backup/recovery repository
// configuration and the local object-store contract used by Cluster Backup.
// A Repository is a cluster resource; it is not an Engine or Module store.
package repository

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
)

type Type string

const (
	TypeFile Type = "file"
	TypeS3   Type = "s3"
)

var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

// Repository is the cluster-persisted definition of one backup/recovery
// destination. URI is intentionally not used: file and S3 need different
// non-secret options, while credentials must never be encoded in a URL.
//
// EncryptedCredentials is an opaque authenticated-encryption envelope. The
// Repository record never contains a plaintext access key, secret, password,
// or token. The encryption key lifecycle is owned by the recovery platform.
type Repository struct {
	Name   string `json:"name"`
	Type   Type   `json:"type"`
	Path   string `json:"path,omitempty"`
	Bucket string `json:"bucket,omitempty"`
	Prefix string `json:"prefix,omitempty"`

	Endpoint  string `json:"endpoint,omitempty"`
	Region    string `json:"region,omitempty"`
	PathStyle bool   `json:"path_style,omitempty"`

	EncryptedCredentials  []byte `json:"encrypted_credentials,omitempty"`
	CredentialsNonce      []byte `json:"credentials_nonce,omitempty"`
	CredentialsAlgorithm  string `json:"credentials_algorithm,omitempty"`
	CredentialsKeyVersion uint32 `json:"credentials_key_version,omitempty"`
}

func ValidateName(value string) error {
	if !namePattern.MatchString(value) {
		return errors.New("invalid Repository name")
	}
	return nil
}

func (value Repository) Validate() error {
	if err := ValidateName(value.Name); err != nil {
		return err
	}
	switch value.Type {
	case TypeFile:
		if value.Path == "" || !filepath.IsAbs(value.Path) || filepath.Clean(value.Path) != value.Path {
			return errors.New("file Repository requires a clean absolute path")
		}
		if value.Bucket != "" || value.Prefix != "" || value.Endpoint != "" || value.Region != "" || value.PathStyle {
			return errors.New("file Repository contains S3 options")
		}
	case TypeS3:
		if value.Bucket == "" || len(value.Bucket) > 255 || strings.ContainsAny(value.Bucket, "/\\") {
			return errors.New("S3 Repository requires a valid bucket")
		}
		if strings.HasPrefix(value.Prefix, "/") || strings.ContainsRune(value.Prefix, '\x00') {
			return errors.New("S3 Repository prefix is invalid")
		}
		if value.Endpoint != "" {
			parsed, err := url.Parse(value.Endpoint)
			if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
				return errors.New("S3 Repository endpoint must be an HTTP(S) origin without credentials or query parameters")
			}
		}
		if value.Path != "" {
			return errors.New("S3 Repository contains a file path")
		}
	default:
		return fmt.Errorf("unsupported Repository type %q", value.Type)
	}
	if len(value.EncryptedCredentials) == 0 {
		if len(value.CredentialsNonce) != 0 || value.CredentialsAlgorithm != "" || value.CredentialsKeyVersion != 0 {
			return errors.New("Repository credential envelope is incomplete")
		}
	} else {
		if len(value.EncryptedCredentials) > 64<<10 || len(value.CredentialsNonce) != 12 || value.CredentialsAlgorithm != CredentialAlgorithmAES256GCMName || value.CredentialsKeyVersion == 0 {
			return errors.New("Repository credential envelope is invalid")
		}
	}
	return nil
}

func (value Repository) IsFile() bool { return value.Type == TypeFile }
func (value Repository) IsS3() bool   { return value.Type == TypeS3 }
