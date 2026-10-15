package rootstate

import (
	"encoding/hex"
	"errors"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/identity"
)

const (
	CreateJoinGrant     = "create_join_grant"
	ConsumeJoinGrant    = "consume_join_grant"
	RevokeJoinGrant     = "revoke_join_grant"
	JoinGrantActive     = "active"
	JoinGrantConsumed   = "consumed"
	JoinGrantRevoked    = "revoked"
	DefaultJoinGrantTTL = identity.DefaultJoinGrantTTL
	MaxJoinGrantTTL     = identity.MaxJoinGrantTTL
)

// JoinGrant contains only public authorization metadata. Neither the random
// secret nor the enrollment/Node/CA private keys may enter Root commands,
// snapshots or results. Enrollment and Node certificates have separate roles.
type JoinGrant struct {
	GrantID             string `json:"grant_id"`
	SecretSHA256        string `json:"secret_sha256"`
	EnrollmentKeySHA256 string `json:"enrollment_key_sha256"`
	IssuerRaftID        uint64 `json:"issuer_raft_id"`
	CreatedUnix         int64  `json:"created_unix"`
	ExpiresUnix         int64  `json:"expires_unix"`
	State               string `json:"state"`
	BoundRequestSHA256  string `json:"bound_request_sha256,omitempty"`
	ConsumedUnix        int64  `json:"consumed_unix,omitempty"`
	RevokedUnix         int64  `json:"revoked_unix,omitempty"`
}

type JoinGrantConsumption struct {
	GrantID        string `json:"grant_id"`
	RequestSHA256  string `json:"request_sha256"`
	AuthorizedUnix int64  `json:"authorized_unix"`
}

type JoinGrantRevocation struct {
	GrantID     string `json:"grant_id"`
	RevokedUnix int64  `json:"revoked_unix"`
}

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == value
}

func (g JoinGrant) Validate() error {
	if identity.ValidateJoinGrantID(g.GrantID) != nil || !validDigest(g.SecretSHA256) || !validDigest(g.EnrollmentKeySHA256) ||
		g.IssuerRaftID == 0 || g.CreatedUnix <= 0 || g.ExpiresUnix <= g.CreatedUnix ||
		g.ExpiresUnix-g.CreatedUnix > int64(MaxJoinGrantTTL/time.Second) {
		return errors.New("invalid Join Grant identity, digests or validity window")
	}
	switch g.State {
	case JoinGrantActive:
		if g.BoundRequestSHA256 != "" || g.ConsumedUnix != 0 || g.RevokedUnix != 0 {
			return errors.New("active Join Grant has terminal state")
		}
	case JoinGrantConsumed:
		if !validDigest(g.BoundRequestSHA256) || g.ConsumedUnix < g.CreatedUnix || g.ConsumedUnix >= g.ExpiresUnix || g.RevokedUnix != 0 {
			return errors.New("invalid consumed Join Grant binding")
		}
	case JoinGrantRevoked:
		if g.BoundRequestSHA256 != "" || g.ConsumedUnix != 0 || g.RevokedUnix < g.CreatedUnix {
			return errors.New("invalid revoked Join Grant")
		}
	default:
		return errors.New("unknown Join Grant state")
	}
	return nil
}

func joinGrantKey(id string) []byte { return []byte("data/join-grant/" + id) }

func (s *State) JoinGrant(id string) (JoinGrant, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if identity.ValidateJoinGrantID(id) != nil {
		return JoinGrant{}, false, errors.New("invalid Join Grant identity")
	}
	encoded, found, err := s.get(joinGrantKey(id))
	if err != nil || !found {
		return JoinGrant{}, found, err
	}
	var grant JoinGrant
	if err := decodeStreamJSON(encoded, &grant); err != nil {
		return JoinGrant{}, false, err
	}
	if err := grant.Validate(); err != nil {
		return JoinGrant{}, false, err
	}
	if grant.GrantID != id {
		return JoinGrant{}, false, errors.New("Join Grant key does not match record")
	}
	return grant, true, nil
}

// No clock, crypto randomness or secret validation occurs during Apply.
// The authenticated authority supplies its admission time; Raft checks the
// deterministic window and permanently binds one exact canonical request.
func (s *State) applyJoinGrant(batch *pebble.Batch, command Command) (string, error) {
	if command.Type == CreateJoinGrant {
		grant := *command.JoinGrant
		if _, found, err := s.get(nodeKey(grant.IssuerRaftID)); err != nil {
			return "", err
		} else if !found {
			return "Join Grant issuer is not registered", nil
		}
		if _, found, err := s.get(joinGrantKey(grant.GrantID)); err != nil {
			return "", err
		} else if found {
			return "Join Grant is already registered", nil
		}
		return "", setJSON(batch, joinGrantKey(grant.GrantID), grant)
	}
	id := ""
	if command.Type == ConsumeJoinGrant {
		id = command.JoinConsumption.GrantID
	} else {
		id = command.JoinRevocation.GrantID
	}
	encoded, found, err := s.get(joinGrantKey(id))
	if err != nil {
		return "", err
	}
	if !found {
		return "Join Grant is not registered", nil
	}
	var grant JoinGrant
	if err := decodeStreamJSON(encoded, &grant); err != nil {
		return "", err
	}
	if err := grant.Validate(); err != nil {
		return "", err
	}
	if command.Type == ConsumeJoinGrant {
		use := command.JoinConsumption
		if grant.State == JoinGrantRevoked {
			return "Join Grant was revoked", nil
		}
		if use.AuthorizedUnix < grant.CreatedUnix || use.AuthorizedUnix >= grant.ExpiresUnix {
			return "Join Grant is outside its validity window", nil
		}
		if grant.State == JoinGrantConsumed {
			if grant.BoundRequestSHA256 != use.RequestSHA256 {
				return "Join Grant is bound to a different request", nil
			}
			return "", nil
		}
		grant.State, grant.BoundRequestSHA256, grant.ConsumedUnix = JoinGrantConsumed, use.RequestSHA256, use.AuthorizedUnix
	} else {
		if grant.State == JoinGrantConsumed {
			return "consumed Join Grant cannot be revoked", nil
		}
		if grant.State == JoinGrantRevoked {
			return "", nil
		}
		if command.JoinRevocation.RevokedUnix < grant.CreatedUnix {
			return "Join Grant revocation predates creation", nil
		}
		grant.State, grant.RevokedUnix = JoinGrantRevoked, command.JoinRevocation.RevokedUnix
	}
	return "", setJSON(batch, joinGrantKey(id), grant)
}
