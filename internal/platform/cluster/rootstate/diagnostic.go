package rootstate

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cockroachdb/pebble"
	"slices"
	"sort"
	"unicode/utf8"
)

const SaveDiagnostic = "save_diagnostic"
const MaxDiagnosticRecords = 50

// Diagnostic records contain bounded, sanitized observations, never user data,
// raw logs, credentials, filesystem contents or stack traces.
type DiagnosticCheck struct {
	Kind    string `json:"kind"`
	State   string `json:"state"`
	Message string `json:"message"`
}
type DiagnosticNode struct {
	NodeID   string            `json:"node_id"`
	Name     string            `json:"name"`
	RegionID string            `json:"region_id"`
	Checks   []DiagnosticCheck `json:"checks"`
}
type DiagnosticRecord struct {
	ID           string           `json:"id"`
	RegionID     string           `json:"region_id,omitempty"`
	NodeID       string           `json:"node_id,omitempty"`
	CreatedUnix  int64            `json:"created_unix"`
	CreatedIndex uint64           `json:"created_index"`
	Nodes        []DiagnosticNode `json:"nodes"`
}

func (d DiagnosticRecord) Validate() error {
	if !validID(d.ID) || (d.RegionID != "" && !validID(d.RegionID)) || (d.NodeID != "" && !nodeIdentity.MatchString(d.NodeID)) || d.CreatedUnix <= 0 || len(d.Nodes) == 0 || len(d.Nodes) > 64 {
		return errors.New("invalid diagnostic record")
	}
	seen := map[string]bool{}
	for _, n := range d.Nodes {
		if !nodeIdentity.MatchString(n.NodeID) || !validID(n.RegionID) || !utf8.ValidString(n.Name) || len(n.Name) > 256 || seen[n.NodeID] || len(n.Checks) != 4 || (d.NodeID != "" && n.NodeID != d.NodeID) || (d.RegionID != "" && n.RegionID != d.RegionID) {
			return errors.New("invalid diagnostic node")
		}
		seen[n.NodeID] = true
		kinds := map[string]bool{}
		for _, c := range n.Checks {
			if !slices.Contains([]string{"reachability", "readiness", "volumes", "maintenance"}, c.Kind) || c.Kind == "" || kinds[c.Kind] || (c.State != "passed" && c.State != "warning" && c.State != "unknown") || len(c.Message) > 256 || !utf8.ValidString(c.Message) {
				return errors.New("invalid diagnostic check")
			}
			kinds[c.Kind] = true
		}
	}
	b, _ := json.Marshal(d)
	if len(b) > 64<<10 {
		return errors.New("diagnostic record too large")
	}
	return nil
}
func diagnosticKey(id string) []byte { return []byte("data/diagnostic/" + id) }
func (s *State) diagnostics() ([]DiagnosticRecord, error) {
	out := []DiagnosticRecord{}
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("data/diagnostic/"), UpperBound: []byte("data/diagnostic0")})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		var d DiagnosticRecord
		if err := json.Unmarshal(it.Value(), &d); err != nil || d.Validate() != nil || len(out) >= MaxDiagnosticRecords {
			return nil, errors.New("invalid diagnostic directory")
		}
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedIndex > out[j].CreatedIndex })
	return out, it.Error()
}
func (s *State) Diagnostics() ([]DiagnosticRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return nil, err
	}
	return s.diagnostics()
}
func (s *State) applyDiagnostic(batch *pebble.Batch, d DiagnosticRecord) (string, error) {
	if _, found, err := s.get(diagnosticKey(d.ID)); err != nil {
		return "", err
	} else if found {
		return "Diagnostic already exists", nil
	}
	all, err := s.diagnostics()
	if err != nil {
		return "", err
	}
	if len(all) >= MaxDiagnosticRecords {
		if err := batch.Delete(diagnosticKey(all[len(all)-1].ID), nil); err != nil {
			return "", err
		}
	}
	d.CreatedIndex = s.applied + 1
	if err := setJSON(batch, diagnosticKey(d.ID), d); err != nil {
		return "", fmt.Errorf("save diagnostic: %w", err)
	}
	return "", nil
}
