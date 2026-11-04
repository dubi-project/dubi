package resource

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestMaintenanceAdmissionForegroundAndRecovery(t *testing.T) {
	m := NewMaintenance(1000)
	foreground := m.Foreground()
	if _, _, err := m.TryStart(context.Background(), "repair"); !errors.Is(err, ErrMaintenanceBusy) {
		t.Fatal(err)
	}
	foreground()
	foreground()
	ctx, finish, err := m.TryStart(context.Background(), "repair")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.TryStart(context.Background(), "backup"); !errors.Is(err, ErrMaintenanceBusy) {
		t.Fatal(err)
	}
	if err := WaitIO(ctx, 1000); err != nil {
		t.Fatal(err)
	}
	wait, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	if err := WaitIO(wait, 1000); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("unbounded IO admitted", err)
	}
	if err := WaitIO(context.Background(), 1<<30); err != nil {
		t.Fatal("foreground was throttled", err)
	}
	finish()
	finish()
	if m.Status().Active != "" || m.Status().Foreground != 0 || m.Status().AccountedBytes != 1000 {
		t.Fatal(m.Status())
	}
	if _, done, err := m.TryStart(context.Background(), "backup"); err != nil {
		t.Fatal(err)
	} else {
		done()
	}
}

func TestMaintenanceBusyForegroundStillAllowsBoundedBackgroundProgress(t *testing.T) {
	m := NewMaintenance(4000)
	done := m.Foreground()
	defer done()
	m.mu.Lock()
	m.lastStart = time.Now().Add(-time.Minute)
	m.mu.Unlock()
	ctx, finish, err := m.TryStart(context.Background(), "repair")
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	if err := WaitIO(ctx, 1000); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := WaitIO(short, 1000); err == nil {
		t.Fatal("foreground did not reduce background bandwidth")
	}
}

func TestMaintenanceReaderCancelClosesSource(t *testing.T) {
	m := NewMaintenance(1)
	ctx, finish, err := m.TryStart(context.Background(), "backup")
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	reader := LimitReader(ctx, io.NopCloser(strings.NewReader("backup")))
	if _, err := reader.Read(make([]byte, 128)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCooperatingLocalRepairSharesActiveGeoIOBucket(t *testing.T) {
	m := NewMaintenance(1000)
	geo, finish, err := m.TryStart(context.Background(), "geo-repair")
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	if err := WaitIO(geo, 1000); err != nil {
		t.Fatal(err)
	}
	local, cancel := context.WithTimeout(WithMaintenance(context.Background(), m), 25*time.Millisecond)
	defer cancel()
	if err := WaitIO(local, 1000); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("local repair bypassed shared IO limit", err)
	}
	if m.Status().Active != "geo-repair" || m.Status().AccountedBytes != 1000 {
		t.Fatal("cooperative work changed another job or IO accounting", m.Status())
	}
}
