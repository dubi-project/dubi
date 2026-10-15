// Package raft owns DUBI's consensus Group persistence boundaries.
package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	RootGroupID     = "root"
	MaxGroupIDBytes = 128
	groupDirectory  = "dubi-raft-groups"
	identityFile    = "GROUP_IDENTITY"
	identityVersion = 2
)

type diskIdentity struct {
	Version    int    `json:"version"`
	GroupID    string `json:"group_id"`
	ResourceID string `json:"resource_id"`
}

// HasGroupResources is a read-only guard for fresh enrollment. Any entry in
// the consensus directory (including an incomplete Group) is existing state,
// never permission to recreate a missing admission/activation fence.
func HasGroupResources(storeRoot string) (bool, error) {
	if storeRoot == "" {
		return false, errors.New("Store root must not be empty")
	}
	if _, err := os.Lstat(filepath.Join(storeRoot, groupRegistryFile)); err == nil {
		return true, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	parent := filepath.Join(storeRoot, groupDirectory)
	info, err := os.Lstat(parent)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil || !info.IsDir() {
		return false, errors.Join(err, errors.New("Raft Group parent must be a physical directory"))
	}
	entries, err := os.ReadDir(parent)
	return len(entries) != 0, err
}

// ValidateGroupID rejects path separators and non-canonical Group IDs.
func ValidateGroupID(groupID string) error {
	if groupID == "" || groupID == "." || groupID == ".." || len(groupID) > MaxGroupIDBytes || strings.TrimSpace(groupID) != groupID {
		return fmt.Errorf("Raft GroupID length must be between 1 and %d bytes without surrounding whitespace", MaxGroupIDBytes)
	}
	for _, character := range groupID {
		if !((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '-' || character == '_' || character == '.') {
			return fmt.Errorf("Raft GroupID %q contains an unsupported character", groupID)
		}
	}
	return nil
}

// PrepareGroupRoot registers an isolated directory before its Raft log opens.
// The Store-level catalog survives loss of the entire Group directory. A
// missing active resource is never re-created; only pending creation resumes.
func PrepareGroupRoot(storeRoot, groupID string) (string, error) {
	return prepareRegisteredGroup(storeRoot, groupID, nil)
}

func decodeIdentity(encoded []byte) (diskIdentity, error) {
	var value diskIdentity
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return diskIdentity{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return diskIdentity{}, errors.New("unexpected trailing Group identity data")
	}
	if value.Version != identityVersion || ValidateGroupID(value.GroupID) != nil || !validGroupResourceID(value.ResourceID) {
		return diskIdentity{}, errors.New("invalid Raft Group identity")
	}
	return value, nil
}
