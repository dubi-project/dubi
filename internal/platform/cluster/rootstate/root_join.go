package rootstate

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
)

const (
	BeginRootJoin    = "begin_root_join"
	CompleteRootJoin = "complete_root_join"
	RootJoinPending  = "pending"
	RootJoinAdmitted = "admitted"
)

// RootJoin is an internal durable control task, not another user-configured
// identity. Its GrantID already identifies the one authorized CSR. A completed
// task must never be used to silently re-add a subsequently removed member.
type RootJoin struct {
	GrantID            string `json:"grant_id"`
	RequestSHA256      string `json:"request_sha256"`
	RegistrationSHA256 string `json:"registration_sha256"`
	NodeID             string `json:"node_id"`
	RaftID             uint64 `json:"raft_id"`
	State              string `json:"state"`
	AdmittedIndex      uint64 `json:"admitted_index,omitempty"`
}

type RootJoinStart struct {
	GrantID string                     `json:"grant_id"`
	Request identityenrollment.Request `json:"request"`
}

type RootJoinFinish struct {
	GrantID string `json:"grant_id"`
}

func (r RootJoin) Validate() error {
	if identity.ValidateJoinGrantID(r.GrantID) != nil || !validDigest(r.RequestSHA256) || !validDigest(r.RegistrationSHA256) || !nodeIdentity.MatchString(r.NodeID) || r.RaftID == 0 {
		return errors.New("invalid Root join task identity or request binding")
	}
	if r.State == RootJoinPending && r.AdmittedIndex == 0 || r.State == RootJoinAdmitted && r.AdmittedIndex > 0 {
		return nil
	}
	return errors.New("invalid Root join task state")
}

func (r RootJoinStart) validate() error {
	if identity.ValidateJoinGrantID(r.GrantID) != nil || r.Request.ServerName != nodepki.DefaultServerName {
		return errors.New("invalid Root join start")
	}
	encoded, err := identityenrollment.EncodeRequest(r.Request)
	if err != nil {
		return err
	}
	if len(encoded) > 16<<10 {
		return errors.New("Root join enrollment request exceeds limit")
	}
	return nil
}

func rootJoinKey(id string) []byte { return []byte("data/root-join/" + id) }

// NodeEnrollmentCommand binds the registration result to the complete CSR and
// metadata digest. Keep the full hash: registration itself stores only Node
// metadata, so truncating this key would truncate the CSR binding as well.
func NodeEnrollmentCommand(request identityenrollment.Request) (Command, error) {
	encoded, err := identityenrollment.EncodeRequest(request)
	if err != nil {
		return Command{}, err
	}
	digest := sha256.Sum256(encoded)
	return Command{Version: FormatVersion, RequestID: "enroll-node-" + hex.EncodeToString(digest[:]), Type: RegisterNode,
		Node: &Node{NodeID: request.NodeID, RaftID: request.RaftID, RegionID: request.RegionID, Address: request.Address}}, nil
}

func (s *State) RootJoin(grantID string) (RootJoin, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if identity.ValidateJoinGrantID(grantID) != nil {
		return RootJoin{}, false, errors.New("invalid Root join Grant identity")
	}
	return s.rootJoin(grantID)
}

func (s *State) rootJoin(grantID string) (RootJoin, bool, error) {
	encoded, found, err := s.get(rootJoinKey(grantID))
	if err != nil || !found {
		return RootJoin{}, found, err
	}
	var task RootJoin
	if err := decodeStreamJSON(encoded, &task); err != nil {
		return RootJoin{}, false, err
	}
	if err := task.Validate(); err != nil {
		return RootJoin{}, false, err
	}
	if task.GrantID != grantID {
		return RootJoin{}, false, errors.New("Root join key differs from record")
	}
	return task, true, nil
}

func (s *State) PendingRootJoins() ([]RootJoin, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return nil, err
	}
	iterator, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("data/root-join/"), UpperBound: []byte("data/root-join0")})
	if err != nil {
		return nil, err
	}
	defer iterator.Close()
	var tasks []RootJoin
	for iterator.First(); iterator.Valid(); iterator.Next() {
		var task RootJoin
		if err := decodeStreamJSON(iterator.Value(), &task); err != nil {
			return nil, err
		}
		if err := task.Validate(); err != nil {
			return nil, err
		}
		if string(rootJoinKey(task.GrantID)) != string(iterator.Key()) {
			return nil, errors.New("Root join key differs from record")
		}
		if task.State == RootJoinPending {
			tasks = append(tasks, task)
		}
	}
	return tasks, iterator.Error()
}

func (s *State) applyRootJoin(batch *pebble.Batch, command Command) (string, error) {
	if command.Type == BeginRootJoin {
		start := command.RootJoinStart
		encoded, err := identityenrollment.EncodeRequest(start.Request)
		if err != nil {
			return "", err
		}
		digest := sha256.Sum256(encoded)
		requestHash := hex.EncodeToString(digest[:])
		grantBytes, found, err := s.get(joinGrantKey(start.GrantID))
		if err != nil {
			return "", err
		}
		if !found {
			return "Root join has no consumed enrollment grant", nil
		}
		var grant JoinGrant
		if err := decodeStreamJSON(grantBytes, &grant); err != nil {
			return "", err
		}
		if err := grant.Validate(); err != nil {
			return "", err
		}
		if grant.State != JoinGrantConsumed || grant.BoundRequestSHA256 != requestHash {
			return "Root join does not match the consumed enrollment request", nil
		}
		existing, found, err := s.rootJoin(start.GrantID)
		if err != nil {
			return "", err
		}
		if found {
			if existing.RequestSHA256 != requestHash || existing.NodeID != start.Request.NodeID || existing.RaftID != start.Request.RaftID {
				return "Root join task belongs to another Node", nil
			}
			return "", nil // A completed task is immutable, never reset to Pending.
		}
		nodeBytes, found, err := s.get(nodeKey(start.Request.RaftID))
		if err != nil {
			return "", err
		}
		if !found {
			return "Root join Node is not registered", nil
		}
		var node Node
		if err := decodeStreamJSON(nodeBytes, &node); err != nil {
			return "", err
		}
		if node.NodeID != start.Request.NodeID || node.RaftID != start.Request.RaftID || node.RegionID != start.Request.RegionID || node.Address != start.Request.Address {
			return "Root join metadata differs from enrolled Node", nil
		}
		if _, fenced, err := s.nodeFence(node.RaftID); err != nil {
			return "", err
		} else if fenced {
			return "Root join Node identity is fenced", nil
		}
		registration, err := NodeEnrollmentCommand(start.Request)
		if err != nil {
			return "", err
		}
		registrationBytes, err := EncodeCommand(registration)
		if err != nil {
			return "", err
		}
		resultBytes, found, err := s.get(resultKey(registration.RequestID))
		if err != nil {
			return "", err
		}
		if !found {
			return "Root join has no exact CSR registration result", nil
		}
		var result Result
		if err := decodeStreamJSON(resultBytes, &result); err != nil {
			return "", err
		}
		registrationHash := sha256.Sum256(registrationBytes)
		if result.Error != "" || result.RequestID != registration.RequestID || result.Hash != hex.EncodeToString(registrationHash[:]) {
			return "Root join CSR registration was not confirmed", nil
		}
		task := RootJoin{GrantID: start.GrantID, RequestSHA256: requestHash, RegistrationSHA256: hex.EncodeToString(registrationHash[:]), NodeID: node.NodeID, RaftID: node.RaftID, State: RootJoinPending}
		return "", setJSON(batch, rootJoinKey(start.GrantID), task)
	}
	grantID := command.RootJoinFinish.GrantID
	task, found, err := s.rootJoin(grantID)
	if err != nil {
		return "", err
	}
	if !found {
		return "Root join task does not exist", nil
	}
	if task.State == RootJoinAdmitted {
		return "", nil
	}
	if _, fenced, err := s.nodeFence(task.RaftID); err != nil {
		return "", err
	} else if fenced {
		return "Root join Node identity is fenced", nil
	}
	member := false
	for _, ids := range [][]uint64{s.conf.GetVoters(), s.conf.GetVotersOutgoing(), s.conf.GetLearners(), s.conf.GetLearnersNext()} {
		for _, id := range ids {
			if id == task.RaftID {
				member = true
			}
		}
	}
	if !member {
		return "Root join Node is not yet an applied Root member", nil
	}
	task.State, task.AdmittedIndex = RootJoinAdmitted, s.applied+1
	return "", setJSON(batch, rootJoinKey(grantID), task)
}
