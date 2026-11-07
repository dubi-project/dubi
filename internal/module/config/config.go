// Package config owns application configuration. Node startup configuration
// remains in platform/config and is unrelated to this Module.
package config

import (
	"context"
	"unicode/utf8"

	"github.com/dubi-project/dubi/internal/engine/state"
	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

const ModuleID = "config"

type Service struct{ state *state.Engine }

func New(engine *state.Engine) *Service { return &Service{state: engine} }

func (s *Service) client(principal auth.Principal, key string) (*state.Client, error) {
	if principal.TenantID() == "" || principal.CredentialID() == "" {
		return nil, auth.ErrInvalidCredential
	}
	if s == nil || s.state == nil || key == "" || len(key) > state.MaxKeyBytes || !utf8.ValidString(key) {
		return nil, state.ErrInvalid
	}
	if err := principal.Authorize(rootstate.Keyspace{TenantID: principal.TenantID(), ModuleID: ModuleID, EngineID: state.EngineID}); err != nil {
		return nil, err
	}
	return s.state.ForTenant(principal.TenantID())
}
func (s *Service) Get(ctx context.Context, principal auth.Principal, key string) (state.Entry, error) {
	c, err := s.client(principal, key)
	if err != nil {
		return state.Entry{}, err
	}
	return c.Get(ctx, []byte(key))
}
func (s *Service) Put(ctx context.Context, principal auth.Principal, key string, value []byte, requestID string, condition state.Condition) (state.Result, error) {
	if err := principal.AuthorizeWrite(); err != nil {
		return state.Result{}, err
	}
	c, err := s.client(principal, key)
	if err != nil {
		return state.Result{}, err
	}
	return c.Put(ctx, []byte(key), value, requestID, condition)
}
func (s *Service) Delete(ctx context.Context, principal auth.Principal, key, requestID string, condition state.Condition) (state.Result, error) {
	if err := principal.AuthorizeWrite(); err != nil {
		return state.Result{}, err
	}
	c, err := s.client(principal, key)
	if err != nil {
		return state.Result{}, err
	}
	return c.Delete(ctx, []byte(key), requestID, condition)
}

func (s *Service) List(ctx context.Context, principal auth.Principal, request state.ListRequest) (state.ListPage, error) {
	// Reuse the same bound module authorization; the placeholder is never stored.
	c, err := s.client(principal, "list")
	if err != nil {
		return state.ListPage{}, err
	}
	return c.List(ctx, request)
}
