package rootstate

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/pebble"
)

const UpdateNotifications = "update_notifications"
const MarkNotifications = "mark_notifications"
const MaxNotifications = 500
const MaxNotificationSources = 4096

type Notification struct {
	ID           string `json:"id"`
	Kind         string `json:"kind"`
	Level        string `json:"level"`
	Title        string `json:"title"`
	Summary      string `json:"summary"`
	Detail       string `json:"detail"`
	Advice       string `json:"advice"`
	Source       string `json:"source"`
	Region       string `json:"region"`
	ResourceID   string `json:"resource_id"`
	Target       string `json:"target"`
	CreatedUnix  int64  `json:"created_unix"`
	CreatedIndex uint64 `json:"created_index"`
}
type NotificationObservation struct {
	Key   string        `json:"key"`
	State string        `json:"state"`
	Event *Notification `json:"event,omitempty"`
	// Initial healthy observations establish a baseline without recovery messages.
	SuppressInitial bool `json:"suppress_initial,omitempty"`
}
type NotificationUpdate struct {
	Revision     uint64                    `json:"revision"`
	Scope        string                    `json:"scope"`
	ObservedUnix int64                     `json:"observed_unix"`
	Observations []NotificationObservation `json:"observations"`
}
type NotificationFeed struct {
	Revision uint64                       `json:"revision"`
	Items    []Notification               `json:"items"`
	Sources  map[string]map[string]string `json:"sources"`
}
type NotificationMark struct {
	CredentialID string `json:"credential_id"`
	ID           string `json:"id,omitempty"`
	All          bool   `json:"all,omitempty"`
	Read         bool   `json:"read"`
	ThroughIndex uint64 `json:"through_index,omitempty"`
}
type NotificationReads struct {
	CredentialID string   `json:"credential_id"`
	IDs          []string `json:"ids"`
}

func notificationSourceKey(s string) bool { return len(s) > 0 && notificationText(s, 256) }
func notificationScope(s string) bool {
	return slices.Contains([]string{"node", "volume", "tenant", "credential", "backup"}, s)
}
func notificationText(s string, max int) bool {
	return utf8.ValidString(s) && len(s) <= max && !strings.ContainsRune(s, '\x00')
}
func (n Notification) Validate() error {
	if !notificationScope(n.Kind) || !slices.Contains([]string{"warning", "error", "success", "info"}, n.Level) || !slices.Contains([]string{"globe", "volume", "tenant", "key", "archive", "tool"}, n.Target) || n.Title == "" || !notificationText(n.Title, 512) || !notificationText(n.Summary, 1024) || !notificationText(n.Detail, 2048) || !notificationText(n.Advice, 1024) || !notificationText(n.Source, 512) || !notificationText(n.Region, 256) || !notificationText(n.ResourceID, 256) {
		return errors.New("invalid notification")
	}
	return nil
}
func (u NotificationUpdate) Validate() error {
	if !notificationScope(u.Scope) || u.ObservedUnix <= 0 || len(u.Observations) > MaxNotificationSources {
		return errors.New("invalid notification observation")
	}
	seen := map[string]bool{}
	for _, o := range u.Observations {
		if !notificationSourceKey(o.Key) || !validID(o.State) || seen[o.Key] {
			return errors.New("invalid notification source")
		}
		seen[o.Key] = true
		if o.Event != nil && (o.Event.Validate() != nil || o.Event.Kind != u.Scope || o.Event.ID != "" || o.Event.CreatedIndex != 0 || o.Event.CreatedUnix != 0) {
			return errors.New("invalid notification event")
		}
	}
	b, _ := json.Marshal(u)
	if len(b) > 512<<10 {
		return errors.New("notification observations too large")
	}
	return nil
}
func (m NotificationMark) Validate() error {
	if !validTenantCredentialID(m.CredentialID) || (m.All && (m.ID != "" || !m.Read)) || (!m.All && (!validID(m.ID) || m.ThroughIndex != 0)) {
		return errors.New("invalid notification read mutation")
	}
	return nil
}
func (f NotificationFeed) Validate(index uint64) error {
	if f.Sources == nil || (len(f.Items) > 0 && f.Revision == 0) || f.Revision > index || len(f.Items) > MaxNotifications || len(f.Sources) > 5 {
		return errors.New("invalid notification feed")
	}
	seen := map[string]bool{}
	var prev uint64 = ^uint64(0)
	for _, n := range f.Items {
		if n.Validate() != nil || !validID(n.ID) || n.CreatedIndex == 0 || n.CreatedIndex > f.Revision || n.CreatedIndex > index || n.CreatedIndex > prev || n.CreatedUnix <= 0 || seen[n.ID] {
			return errors.New("invalid stored notification")
		}
		seen[n.ID] = true
		prev = n.CreatedIndex
	}
	for scope, sources := range f.Sources {
		if !notificationScope(scope) || len(sources) > MaxNotificationSources {
			return errors.New("invalid notification checkpoints")
		}
		for key, state := range sources {
			if !notificationSourceKey(key) || !validID(state) {
				return errors.New("invalid notification checkpoint")
			}
		}
	}
	return nil
}
func notificationFeedKey() []byte          { return []byte("data/notifications") }
func notificationReadKey(id string) []byte { return []byte("data/notification-read/" + id) }
func (s *State) notificationFeed() (NotificationFeed, error) {
	f := NotificationFeed{Items: []Notification{}, Sources: map[string]map[string]string{}}
	b, found, e := s.get(notificationFeedKey())
	if e != nil || !found {
		return f, e
	}
	if json.Unmarshal(b, &f) != nil || f.Validate(s.applied) != nil {
		return f, errors.New("invalid stored notification feed")
	}
	return f, nil
}
func (s *State) NotificationFeed() (NotificationFeed, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e := s.usable(); e != nil {
		return NotificationFeed{}, e
	}
	return s.notificationFeed()
}

// The shared cluster record supersedes legacy per-credential read records.
// Before the first shared write, union retained legacy reads so existing read
// acknowledgements remain read for everyone. The first mutation removes them.
func (s *State) notificationReads(_ string) (NotificationReads, error) {
	r := NotificationReads{IDs: []string{}}
	b, found, e := s.get(notificationReadKey("cluster"))
	if e != nil {
		return r, e
	}
	if found {
		if json.Unmarshal(b, &r) != nil || r.CredentialID != "" || len(r.IDs) > MaxNotifications {
			return r, errors.New("invalid cluster notification reads")
		}
		return r, nil
	}
	feed, e := s.notificationFeed()
	if e != nil {
		return r, e
	}
	retained := map[string]bool{}
	for _, n := range feed.Items {
		retained[n.ID] = true
	}
	reads := map[string]bool{}
	it, e := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("data/notification-read/"), UpperBound: []byte("data/notification-read0")})
	if e != nil {
		return r, e
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		var old NotificationReads
		if json.Unmarshal(it.Value(), &old) != nil || len(old.IDs) > MaxNotifications {
			return r, errors.New("invalid legacy notification reads")
		}
		for _, id := range old.IDs {
			if retained[id] {
				reads[id] = true
			}
		}
	}
	for _, n := range feed.Items {
		if reads[n.ID] {
			r.IDs = append(r.IDs, n.ID)
		}
	}
	return r, it.Error()
}
func (s *State) NotificationReads(id string) (NotificationReads, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e := s.usable(); e != nil {
		return NotificationReads{}, e
	}
	if !validTenantCredentialID(id) {
		return NotificationReads{}, errors.New("invalid credential")
	}
	return s.notificationReads(id)
}
func (s *State) applyNotificationUpdate(batch *pebble.Batch, u NotificationUpdate) (string, error) {
	f, e := s.notificationFeed()
	if e != nil {
		return "", e
	}
	if f.Revision != u.Revision {
		return "Notification revision conflict", nil
	}
	prior, initialized := f.Sources[u.Scope]
	next := map[string]string{}
	events := []Notification{}
	for _, o := range u.Observations {
		next[o.Key] = o.State
		state, found := prior[o.Key]
		if state == o.State || o.Event == nil || (!found && o.SuppressInitial) || (!initialized && u.Scope == "backup") {
			continue
		}
		n := *o.Event
		n.ID = fmt.Sprintf("msg_%x_%x", s.applied+1, len(events))
		n.CreatedIndex = s.applied + 1
		n.CreatedUnix = u.ObservedUnix
		events = append(events, n)
	}
	if initialized && maps.Equal(prior, next) {
		return "", nil
	}
	f.Sources[u.Scope] = next
	f.Revision = s.applied + 1
	// Observations have a canonical ordering; reverse keeps newer generated IDs first.
	slices.Reverse(events)
	f.Items = append(events, f.Items...)
	if len(f.Items) > MaxNotifications {
		f.Items = f.Items[:MaxNotifications]
	}
	return "", setJSON(batch, notificationFeedKey(), f)
}
func (s *State) applyNotificationMark(batch *pebble.Batch, m NotificationMark) (string, error) {
	credential, found, e := s.accessCredential(m.CredentialID)
	if e != nil {
		return "", e
	}
	allowed := found && credential.RevokedIndex == 0 && credential.Role == "admin"
	if found && credential.RevokedIndex == 0 && credential.Role == "session" {
		if credential.AccountID != "" {
			a, ok, err := s.adminAccount(credential.AccountID)
			if err != nil {
				return "", err
			}
			allowed = ok && !a.Disabled && a.AuthVersion == credential.AccountVersion
		} else {
			p, ok, err := s.accessCredential(credential.ParentID)
			if err != nil {
				return "", err
			}
			allowed = ok && p.Role == "admin" && p.RevokedIndex == 0
		}
	}
	if !allowed {
		return "Admin credential is not active", nil
	}
	f, e := s.notificationFeed()
	if e != nil {
		return "", e
	}
	r, e := s.notificationReads(m.CredentialID)
	if e != nil {
		return "", e
	}
	if !m.All && !slices.ContainsFunc(f.Items, func(n Notification) bool { return n.ID == m.ID }) {
		return "Notification is no longer retained", nil
	}
	if m.All && m.ThroughIndex > f.Revision {
		return "Notification read cut is ahead of feed", nil
	}
	reads := map[string]bool{}
	for _, id := range r.IDs {
		reads[id] = true
	}
	r.IDs = []string{}
	for _, n := range f.Items {
		read := reads[n.ID]
		if m.All && n.CreatedIndex <= m.ThroughIndex {
			read = true
		}
		if !m.All && n.ID == m.ID {
			read = m.Read
		}
		if read {
			r.IDs = append(r.IDs, n.ID)
		}
	}
	it, e := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("data/notification-read/"), UpperBound: []byte("data/notification-read0")})
	if e != nil {
		return "", e
	}
	for it.First(); it.Valid(); it.Next() {
		if string(it.Key()) != string(notificationReadKey("cluster")) {
			if e = batch.Delete(it.Key(), nil); e != nil {
				_ = it.Close()
				return "", e
			}
		}
	}
	if e = errors.Join(it.Error(), it.Close()); e != nil {
		return "", e
	}
	r.CredentialID = ""
	return "", setJSON(batch, notificationReadKey("cluster"), r)
}
