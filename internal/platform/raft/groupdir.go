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
	identityVersion = 1
)

type diskIdentity struct {
	Version int    `json:"version"`
	GroupID string `json:"group_id"`
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

// PrepareGroupRoot creates an isolated directory and durable identity marker
// before the Group's Raft log or state machine is opened. It never adopts an
// existing non-empty, unidentified directory.
func PrepareGroupRoot(storeRoot, groupID string) (string, error) {
	if storeRoot == "" {
		return "", errors.New("Store root must not be empty")
	}
	if err := ValidateGroupID(groupID); err != nil {
		return "", err
	}
	parent := filepath.Join(storeRoot, groupDirectory)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return "", fmt.Errorf("create Raft Group parent: %w", err)
	}
	parentInfo, err := os.Lstat(parent)
	if err != nil || !parentInfo.IsDir() || parentInfo.Mode()&os.ModeSymlink != 0 {
		return "", errors.Join(err, errors.New("Raft Group parent is not a directory"))
	}
	root := filepath.Join(parent, groupID)
	if err := os.MkdirAll(root, 0o750); err != nil {
		return "", fmt.Errorf("create Raft Group root: %w", err)
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.Join(err, errors.New("Raft Group root is not a directory"))
	}
	marker := filepath.Join(root, identityFile)
	if encoded, err := os.ReadFile(marker); err == nil {
		value, err := decodeIdentity(encoded)
		if err != nil {
			return "", fmt.Errorf("read Raft Group identity: %w", err)
		}
		if value.GroupID != groupID {
			return "", fmt.Errorf("Raft Group directory is bound to %q, not %q", value.GroupID, groupID)
		}
		return root, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("read Raft Group identity: %w", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return "", fmt.Errorf("inspect new Raft Group root: %w", err)
	}
	if len(entries) != 0 {
		return "", errors.New("refusing to adopt a non-empty Raft Group directory without an identity marker")
	}
	encoded, err := json.Marshal(diskIdentity{Version: identityVersion, GroupID: groupID})
	if err != nil {
		return "", err
	}
	file, err := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		return "", fmt.Errorf("create Raft Group identity: %w", err)
	}
	writeErr := error(nil)
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		writeErr = err
	} else if err := file.Sync(); err != nil {
		writeErr = err
	}
	writeErr = errors.Join(writeErr, file.Close())
	if writeErr != nil {
		return "", fmt.Errorf("persist Raft Group identity: %w", writeErr)
	}
	directory, err := os.Open(root)
	if err != nil {
		return "", fmt.Errorf("open Raft Group root for sync: %w", err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil || closeErr != nil {
		return "", fmt.Errorf("sync Raft Group root: %w", errors.Join(syncErr, closeErr))
	}
	return root, nil
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
	if value.Version != identityVersion || ValidateGroupID(value.GroupID) != nil {
		return diskIdentity{}, errors.New("invalid Raft Group identity")
	}
	return value, nil
}
