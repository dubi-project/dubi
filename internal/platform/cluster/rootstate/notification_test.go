package rootstate

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestNotificationTransitionsReadsRetentionAndSnapshot(t *testing.T) {
	s := rootFileFixture(t)
	admin := func(ch string) string {
		c := AccessCredential{CredentialID: "cred_" + strings.Repeat(ch, 32), Role: "admin", TokenSHA256: strings.Repeat(ch, 64), CreatedUnix: 100, ExpiresUnix: 1000}
		r := applyBackupCommand(t, s, "admin-"+ch, Command{Type: CreateAccessCredential, AccessCredential: &c})
		if r.Error != "" {
			t.Fatal(r)
		}
		return c.CredentialID
	}
	a, b := admin("a"), admin("b")
	n := Notification{Kind: "node", Level: "error", Title: "Node unavailable", Summary: "No response", Target: "globe"}
	update := func(request, state string) {
		f, e := s.NotificationFeed()
		if e != nil {
			t.Fatal(e)
		}
		u := NotificationUpdate{Revision: f.Revision, Scope: "node", ObservedUnix: 100, Observations: []NotificationObservation{{Key: "n1", State: state, Event: &n, SuppressInitial: state == "healthy"}}}
		r := applyBackupCommand(t, s, request, Command{Type: UpdateNotifications, NotificationUpdate: &u})
		if r.Error != "" {
			t.Fatal(r)
		}
	}
	update("baseline", "healthy")
	f, _ := s.NotificationFeed()
	if len(f.Items) != 0 {
		t.Fatal("initial healthy generated recovery")
	}
	old := f.Revision
	update("failure", "unavailable")
	f, _ = s.NotificationFeed()
	if len(f.Items) != 1 {
		t.Fatal("missing failure")
	}
	first := f.Items[0]
	update("repeat", "unavailable")
	f, _ = s.NotificationFeed()
	if len(f.Items) != 1 {
		t.Fatal("repeated alert")
	}
	stale := NotificationUpdate{Revision: old, Scope: "node", ObservedUnix: 99}
	r := applyBackupCommand(t, s, "stale", Command{Type: UpdateNotifications, NotificationUpdate: &stale})
	if r.Error != "Notification revision conflict" {
		t.Fatal("stale leader overwrote checkpoint", r)
	}
	cut := f.Revision
	update("recovery", "healthy")
	f, _ = s.NotificationFeed()
	if len(f.Items) != 2 {
		t.Fatal("missing recovery")
	}
	mark := NotificationMark{CredentialID: a, All: true, Read: true, ThroughIndex: cut}
	r = applyBackupCommand(t, s, "read-cut", Command{Type: MarkNotifications, NotificationMark: &mark})
	if r.Error != "" {
		t.Fatal(r)
	}
	reads, _ := s.NotificationReads(a)
	if len(reads.IDs) != 1 || reads.IDs[0] != first.ID {
		t.Fatal("mark-all consumed newer event", reads)
	}
	other, _ := s.NotificationReads(b)
	if len(other.IDs) != 1 || other.IDs[0] != first.ID {
		t.Fatal("cluster read state not shared")
	}
	applyBackupCommand(t, s, "read-cut", Command{Type: MarkNotifications, NotificationMark: &mark})
	mark = NotificationMark{CredentialID: a, ID: first.ID, Read: false}
	r = applyBackupCommand(t, s, "unread", Command{Type: MarkNotifications, NotificationMark: &mark})
	if r.Error != "" {
		t.Fatal(r)
	}
	reads, _ = s.NotificationReads(a)
	if len(reads.IDs) != 0 {
		t.Fatal("unread failed")
	}
	for i := 0; i < MaxNotifications+2; i++ {
		update(fmt.Sprintf("transition-%d", i), fmt.Sprintf("state-%d", i))
	}
	f, _ = s.NotificationFeed()
	if len(f.Items) != MaxNotifications {
		t.Fatal("retention bound", len(f.Items))
	}
	store := rootArtifacts(t, t.TempDir())
	encoded, index, conf := buildRootFile(t, s, store)
	restored := rootFileState(t, t.TempDir())
	if e := restored.RestoreFrom(context.Background(), store, index, encoded, conf); e != nil {
		t.Fatal(e)
	}
	after, e := restored.NotificationFeed()
	if e != nil || after.Revision != f.Revision || len(after.Items) != MaxNotifications || after.Sources["node"]["n1"] != f.Sources["node"]["n1"] {
		t.Fatal("snapshot lost feed/checkpoints", e)
	}
	current := after.Items[0].ID
	mark = NotificationMark{CredentialID: b, ID: current, Read: true}
	r = applyBackupCommand(t, restored, "restored-read", Command{Type: MarkNotifications, NotificationMark: &mark})
	if r.Error != "" {
		t.Fatal(r)
	}
}
func TestNotificationBackupBaselineAndBatch(t *testing.T) {
	s := rootFileFixture(t)
	n := Notification{Kind: "backup", Level: "success", Title: "Backup completed", Target: "archive"}
	u := NotificationUpdate{Scope: "backup", ObservedUnix: 100, Observations: []NotificationObservation{{Key: "historical", State: "completed", Event: &n}}}
	applyBackupCommand(t, s, "baseline", Command{Type: UpdateNotifications, NotificationUpdate: &u})
	f, _ := s.NotificationFeed()
	if len(f.Items) != 0 {
		t.Fatal("historical backups re-announced")
	}
	u.Revision = f.Revision
	for i := 0; i < 40; i++ {
		u.Observations = append(u.Observations, NotificationObservation{Key: fmt.Sprint("new-", i), State: "completed", Event: &n})
	}
	r := applyBackupCommand(t, s, "new-backups", Command{Type: UpdateNotifications, NotificationUpdate: &u})
	if r.Error != "" {
		t.Fatal(r)
	}
	f, _ = s.NotificationFeed()
	if len(f.Items) != 40 {
		t.Fatal("missing burst events")
	}
	if f.Validate(s.AppliedIndex()) != nil {
		t.Fatal("invalid event ordering")
	}
}
