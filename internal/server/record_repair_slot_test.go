package server

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRecordRepairSlotWaitsForCurrentRepair(t *testing.T) {
	service := &recordRepairService{slots: make(chan struct{}, 1)}
	service.slots <- struct{}{}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	acquired := make(chan error, 1)
	go func() { acquired <- service.acquire(ctx) }()

	select {
	case err := <-acquired:
		t.Fatalf("acquire returned before the active repair released its slot: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	<-service.slots
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("acquire after slot release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("acquire did not proceed after the active repair released its slot")
	}
	<-service.slots
}

func TestRecordRepairSlotHonorsCallerDeadline(t *testing.T) {
	service := &recordRepairService{slots: make(chan struct{}, 1)}
	service.slots <- struct{}{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := service.acquire(ctx)
	if status.Code(err) != codes.Canceled {
		t.Fatalf("acquire error = %v, want Canceled", err)
	}
	if len(service.slots) != 1 {
		t.Fatal("canceled acquire unexpectedly took the active repair slot")
	}
}
