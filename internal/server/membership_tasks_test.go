package server

import (
	"context"
	"errors"
	"testing"
	"time"
)

type rootTaskFunc func(context.Context) error

func (f rootTaskFunc) Reconcile(ctx context.Context) error { return f(ctx) }

type dataTaskFunc func(context.Context) error

func (f dataTaskFunc) ReconcilePending(ctx context.Context) error { return f(ctx) }

func TestRootJoinSchedulerTasksRunIndependentlyAndJoinCancellation(t *testing.T) {
	for _, blocking := range []string{"root", "data"} {
		t.Run(blocking, func(t *testing.T) {
			entered, completed, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
			block := func(ctx context.Context) error { close(entered); <-ctx.Done(); close(stopped); return ctx.Err() }
			finish := func(ctx context.Context) error {
				select {
				case <-entered:
				case <-ctx.Done():
					return ctx.Err()
				}
				close(completed)
				return nil
			}
			tasks := membershipTasks{timeout: time.Second, root: rootTaskFunc(block), data: dataTaskFunc(finish)}
			if blocking == "data" {
				tasks.root, tasks.data = rootTaskFunc(finish), dataTaskFunc(block)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- tasks.ReconcilePending(ctx) }()
			select {
			case <-completed:
			case <-time.After(5 * time.Second):
				t.Fatal("one task blocked the other")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("round did not return after cancellation")
			}
			select {
			case <-stopped:
			default:
				t.Fatal("round returned before worker stopped")
			}
		})
	}
}

func TestRootJoinSchedulerPreservesBothFailuresAndDeadline(t *testing.T) {
	rootFailure, dataFailure := errors.New("root failure"), errors.New("data failure")
	tasks := membershipTasks{root: rootTaskFunc(func(context.Context) error { return rootFailure }), data: dataTaskFunc(func(context.Context) error { return dataFailure })}
	if err := tasks.ReconcilePending(context.Background()); !errors.Is(err, rootFailure) || !errors.Is(err, dataFailure) {
		t.Fatal("lost task failure", err)
	}
	block := func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
	tasks = membershipTasks{timeout: time.Millisecond, root: rootTaskFunc(block), data: dataTaskFunc(block)}
	if err := tasks.ReconcilePending(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("round deadline missing", err)
	}
}

func TestRootControlTasksOrderAndPreserveErrors(t *testing.T) {
	joinFailure, topologyFailure := errors.New("join"), errors.New("topology")
	var order []string
	tasks := rootControlTasks{
		join:     rootTaskFunc(func(context.Context) error { order = append(order, "join"); return joinFailure }),
		topology: rootTaskFunc(func(context.Context) error { order = append(order, "topology"); return topologyFailure }),
	}
	err := tasks.Reconcile(context.Background())
	if !errors.Is(err, joinFailure) || !errors.Is(err, topologyFailure) || len(order) != 2 || order[0] != "join" || order[1] != "topology" {
		t.Fatal("Root control lane lost order/errors", order, err)
	}
}
