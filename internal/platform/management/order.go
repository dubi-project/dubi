package management

import (
	"fmt"
	v1 "github.com/dubi-project/dubi/api/v1"
)

// CreationKey sorts the immutable creation commit newest first, then ID. Zero
// means unrecorded historical metadata and sorts after known creation indices.
func CreationKey(index uint64, id string) string {
	return fmt.Sprintf("%020d/%s", ^uint64(0)-index, id)
}

// CredentialKey retains the recorded issuance time for older credentials. The
// creation commit distinguishes multiple credentials issued in the same second.
func CredentialKey(unix int64, index uint64, id string) string {
	return fmt.Sprintf("%020d/%s", ^uint64(0)-uint64(max(unix, 0)), CreationKey(index, id))
}

// Status is part of both sorting and cursor identity. Within each group keep
// newest issuance first, including legacy rows without a creation index.
func CredentialListKey(c v1.ManagedCredential) string {
	rank := "1"
	if c.Status == "active" {
		rank = "0"
	} else if c.Status == "revoked" {
		rank = "2"
	}
	return rank + "/" + CredentialKey(c.CreatedUnix, c.CreatedIndex, c.CredentialID)
}
