package repository

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	RepositoryKeyFileName = "DUBI_REPOSITORY_KEY"
	repositoryKeyBytes    = 32
)

// EnsureRepositoryKey creates the Store-local key once and returns it. The
// key is deliberately outside Root state and Repository JSON; only encrypted
// credential envelopes enter the replicated cluster metadata.
func EnsureRepositoryKey(store string) ([]byte, error) {
	if err := validateStorePath(store); err != nil {
		return nil, err
	}
	path := filepath.Join(store, RepositoryKeyFileName)
	key := make([]byte, repositoryKeyBytes)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return LoadRepositoryKey(store)
		}
		return nil, fmt.Errorf("create Repository key: %w", err)
	}
	if _, err := file.Write(key); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	if err := syncRepositoryDirectory(store); err != nil {
		return nil, err
	}
	return key, nil
}

// LoadRepositoryKey reads the existing Store-local key. It never generates a
// replacement silently: losing the key is an explicit credential recovery
// operation, not a reason to decrypt old Repository records with a new key.
func LoadRepositoryKey(store string) ([]byte, error) {
	if err := validateStorePath(store); err != nil {
		return nil, err
	}
	path := filepath.Join(store, RepositoryKeyFileName)
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() != repositoryKeyBytes {
		return nil, errors.New("invalid Repository key file")
	}
	key, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(key) != repositoryKeyBytes {
		return nil, errors.New("invalid Repository key length")
	}
	return key, nil
}

func validateStorePath(store string) error {
	if store == "" || !filepath.IsAbs(store) || filepath.Clean(store) != store {
		return errors.New("Repository key requires a clean absolute Store path")
	}
	info, err := os.Stat(store)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("Repository key Store path is not a directory")
	}
	return nil
}

func syncRepositoryDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
