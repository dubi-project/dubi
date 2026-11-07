package management

import (
	"context"
	"errors"
	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"slices"
	"sort"
	"time"
)

type TenantError struct {
	Status        int
	Code, Message string
}

func (e *TenantError) Error() string                     { return e.Message }
func tenantError(status int, code, message string) error { return &TenantError{status, code, message} }

type Tenants struct {
	Root    *rootstate.State
	Barrier func(context.Context) (uint64, error)
	Commit  func(context.Context, rootstate.Command) error
	Modules []v1.TenantModule
}

func (s *Tenants) List(ctx context.Context) ([]v1.ManagedTenant, error) {
	if _, err := s.Barrier(ctx); err != nil {
		return nil, err
	}
	tenants, credentials, err := s.Root.TenantDirectory()
	if err != nil {
		return nil, err
	}
	byTenant := map[string][]v1.TenantCredential{}
	now := time.Now().Unix()
	for _, c := range credentials {
		status := "active"
		if c.RevokedIndex != 0 {
			status = "revoked"
		} else if now < c.CreatedUnix || (c.ExpiresUnix != 0 && now >= c.ExpiresUnix) {
			status = "expired"
		}
		byTenant[c.TenantID] = append(byTenant[c.TenantID], v1.TenantCredential{CreatedIndex: c.CreatedIndex, CredentialID: c.CredentialID, CreatedUnix: c.CreatedUnix, ExpiresUnix: c.ExpiresUnix, Status: status})
	}
	out := make([]v1.ManagedTenant, 0, len(tenants))
	for _, t := range tenants {
		v := v1.ManagedTenant{CreatedIndex: t.CreatedIndex, TenantID: t.TenantID, Name: t.Name, Modules: t.Modules, Revision: t.Revision, StorageLimitBytes: t.StorageLimit, Accounting: t.Accounting, Credentials: byTenant[t.TenantID], Status: "inactive"}
		if v.Name == "" {
			v.Name = t.TenantID
		}
		if v.Modules == nil {
			v.Modules = []string{"config"}
		}
		v.CredentialCount = len(v.Credentials)
		for _, c := range v.Credentials {
			if c.Status == "active" {
				v.ActiveCredentials++
			}
		}
		if len(v.Modules) == 0 {
			v.Status = "unauthorized"
		} else if v.ActiveCredentials > 0 {
			v.Status = "active"
		}
		if t.Accounting {
			u, err := s.Root.TenantUsage(t.TenantID)
			if err != nil {
				return nil, err
			}
			v.UsedBytes = &u.Used
			v.ReservedBytes = &u.Reserved
		}
		sort.Slice(v.Credentials, func(i, j int) bool {
			return CredentialKey(v.Credentials[i].CreatedUnix, v.Credentials[i].CreatedIndex, v.Credentials[i].CredentialID) < CredentialKey(v.Credentials[j].CreatedUnix, v.Credentials[j].CreatedIndex, v.Credentials[j].CredentialID)
		})
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		return CreationKey(out[i].CreatedIndex, out[i].TenantID) < CreationKey(out[j].CreatedIndex, out[j].TenantID)
	})
	return out, nil
}
func (s *Tenants) Get(ctx context.Context, id string) (v1.ManagedTenant, error) {
	all, err := s.List(ctx)
	if err != nil {
		return v1.ManagedTenant{}, err
	}
	for _, v := range all {
		if v.TenantID == id {
			return v, nil
		}
	}
	return v1.ManagedTenant{}, tenantError(404, "not_found", "租户不存在。")
}
func (s *Tenants) Save(ctx context.Context, in v1.TenantMutation, create bool) (v1.ManagedTenant, error) {
	if rootstate.ValidateRequestID(in.RequestID) != nil || rootstate.ValidateTenantID(in.TenantID) != nil || in.Name == "" || in.Modules == nil || (create && in.Revision != nil) || (!create && in.Revision == nil) {
		return v1.ManagedTenant{}, tenantError(400, "invalid_request", "租户标识、名称、模块范围或版本无效。")
	}
	for _, m := range in.Modules {
		if !slices.ContainsFunc(s.Modules, func(v v1.TenantModule) bool { return v.ID == m }) {
			return v1.ManagedTenant{}, tenantError(400, "invalid_request", "所选模块尚未启用。")
		}
	}
	t := rootstate.Tenant{TenantID: in.TenantID, Name: in.Name, Modules: in.Modules, StorageLimit: in.StorageLimitBytes, Accounting: true}
	kind := rootstate.RegisterTenant
	if !create {
		current, err := s.Get(ctx, in.TenantID)
		if err != nil {
			return v1.ManagedTenant{}, err
		}
		t.Accounting = current.Accounting
		t.Revision = *in.Revision
		kind = rootstate.UpdateTenant
	}
	if !t.Accounting && t.StorageLimit > 0 {
		return v1.ManagedTenant{}, tenantError(409, "accounting_unavailable", "此租户没有完整用量记录，无法设置有限配额。")
	}
	if err := t.Validate(); err != nil {
		return v1.ManagedTenant{}, tenantError(400, "invalid_request", err.Error())
	}
	err := s.commit(ctx, rootstate.Command{Version: rootstate.FormatVersion, RequestID: in.RequestID, Type: kind, Tenant: &t})
	if err != nil {
		return v1.ManagedTenant{}, err
	}
	return s.Get(ctx, in.TenantID)
}
func (s *Tenants) commit(ctx context.Context, c rootstate.Command) error {
	err := s.Commit(ctx, c)
	if err == nil {
		return nil
	}
	switch err.Error() {
	case "TenantID is already registered":
		return tenantError(409, "already_exists", "租户标识已存在。")
	case "Tenant revision conflict":
		return tenantError(409, "version_conflict", "租户设置已被修改，请刷新后重试。")
	case "Tenant is not registered", "Tenant credential is not registered":
		return tenantError(404, "not_found", "租户或凭证不存在。")
	case "Tenant credential is already revoked":
		return nil
	}
	// Only a matching durable Root result proves a definitive rejection.
	encoded, encodeErr := rootstate.EncodeCommand(c)
	if encodeErr == nil {
		result, found, lookupErr := s.Root.ResultForCommand(encoded)
		if lookupErr == nil && found && result.Error != "" {
			return tenantError(409, "conflict", result.Error)
		}
		if errors.Is(lookupErr, rootstate.ErrRequestIDReused) {
			return tenantError(409, "request_id_reused", "请求标识已被使用，请核对操作结果。")
		}
	}
	return tenantError(503, "outcome_unknown", "尚未确认操作结果，请保留请求标识重试。")
}
func (s *Tenants) Issue(ctx context.Context, id string, in v1.TenantTokenRequest) (v1.TenantToken, error) {
	if rootstate.ValidateRequestID(in.RequestID) != nil || !validCredentialTTL(in.TTLSeconds, in.NeverExpires) {
		return v1.TenantToken{}, tenantError(400, "invalid_request", "凭证有效期须为 1 秒至 366 天，或明确选择永不过期。")
	}
	if _, err := s.Get(ctx, id); err != nil {
		return v1.TenantToken{}, err
	}
	if _, found, err := s.Root.Result(in.RequestID); err != nil {
		return v1.TenantToken{}, err
	} else if found {
		return v1.TenantToken{}, tenantError(409, "token_not_recoverable", "此请求已经处理，Token 不可再次取回。请核对并撤销不用的凭证。")
	}
	token, c, err := auth.Generate(id, time.Now(), time.Duration(in.TTLSeconds)*time.Second)
	if err != nil {
		return v1.TenantToken{}, err
	}
	if err = s.commit(ctx, rootstate.Command{Version: rootstate.FormatVersion, RequestID: in.RequestID, Type: rootstate.CreateTenantCredential, TenantCredential: &c}); err != nil {
		return v1.TenantToken{}, err
	}
	return v1.TenantToken{CredentialID: c.CredentialID, Token: token, ExpiresUnix: c.ExpiresUnix}, nil
}
func (s *Tenants) Revoke(ctx context.Context, id, credential, request string) error {
	if rootstate.ValidateRequestID(request) != nil || rootstate.ValidateTenantCredentialID(credential) != nil {
		return tenantError(400, "invalid_request", "凭证或请求标识无效。")
	}
	if _, err := s.Barrier(ctx); err != nil {
		return err
	}
	c, ok, err := s.Root.TenantCredential(credential)
	if err != nil {
		return err
	}
	if !ok || c.TenantID != id {
		return tenantError(404, "not_found", "此租户下不存在该凭证。")
	}
	if c.RevokedIndex != 0 {
		return nil
	}
	return s.commit(ctx, rootstate.Command{Version: rootstate.FormatVersion, RequestID: request, Type: rootstate.RevokeTenantCredential, TenantRevocation: &rootstate.TenantCredentialRevocation{CredentialID: credential}})
}
