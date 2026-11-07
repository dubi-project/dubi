package management

import (
	"context"
	"errors"
	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/recovery/repository"
	"sort"
	"strings"
	"time"
)

// Operations shares the existing Root barrier and command path, so any HTTP
// entry observes and mutates the same cluster-owned resources.
type Operations struct {
	Root          *rootstate.State
	Barrier       func(context.Context) (uint64, error)
	Commit        func(context.Context, rootstate.Command) error
	RepositoryKey func() ([]byte, error)
	ClusterID     string
}

func (s *Operations) commit(ctx context.Context, c rootstate.Command) error {
	err := (&Tenants{Root: s.Root, Commit: s.Commit}).commit(ctx, c)
	var failure *TenantError
	if errors.As(err, &failure) {
		messages := map[string]string{"Repository name is already registered": "仓库名称已存在。", "Repository is not registered": "仓库不存在。", "Repository revision conflict": "仓库设置已被修改，请刷新后重试。", "Repository has an active backup": "此仓库有正在执行的备份，请完成后再修改设置。", "Repository destination is used by backups; create a new repository": "已有备份依赖此存储位置；如需更换位置，请添加新仓库。"}
		if translated := messages[failure.Message]; translated != "" {
			return tenantError(failure.Status, failure.Code, translated)
		}
	}
	return err
}
func validCredentialTTL(seconds uint64, never bool) bool {
	if never {
		return seconds == 0
	}
	return seconds >= 1 && seconds <= 366*86400
}

// Delete removes only a revoked record. Root repeats this precondition atomically.
func (s *Operations) DeleteCredential(ctx context.Context, id, request, current string) error {
	if rootstate.ValidateRequestID(request) != nil || rootstate.ValidateTenantCredentialID(id) != nil {
		return tenantError(400, "invalid_request", "请求标识无效。")
	}
	all, e := s.Credentials(ctx, current)
	if e != nil {
		return e
	}
	for _, c := range all {
		if c.CredentialID != id {
			continue
		}
		if c.Status != "revoked" {
			return tenantError(409, "not_revoked", "只有已撤销的凭证可以删除。")
		}
		rev := &rootstate.TenantCredentialRevocation{CredentialID: id}
		command := rootstate.Command{Version: rootstate.FormatVersion, RequestID: request}
		if c.Kind == "tenant" {
			command.Type = rootstate.DeleteTenantCredential
			command.TenantRevocation = rev
		} else {
			command.Type = rootstate.DeleteAccessCredential
			command.AccessRevocation = rev
		}
		return s.commit(ctx, command)
	}
	return nil
}
func credentialState(created, expires int64, revoked uint64) string {
	if revoked != 0 {
		return "revoked"
	}
	if now := time.Now().Unix(); now < created || (expires != 0 && now >= expires) {
		return "expired"
	}
	return "active"
}
func (s *Operations) Credentials(ctx context.Context, current string) ([]v1.ManagedCredential, error) {
	if _, err := s.Barrier(ctx); err != nil {
		return nil, err
	}
	access, err := s.Root.AccessCredentials()
	if err != nil {
		return nil, err
	}
	tenants, credentials, err := s.Root.TenantDirectory()
	if err != nil {
		return nil, err
	}
	out := []v1.ManagedCredential{}
	byTenant := map[string]rootstate.Tenant{}
	for _, t := range tenants {
		if t.Name == "" {
			t.Name = t.TenantID
		}
		if t.Modules == nil {
			t.Modules = []string{"config"}
		}
		byTenant[t.TenantID] = t
	}
	for _, c := range access {
		if c.Role == "session" && c.CredentialID == current {
			current = c.ParentID
		}
	}
	for _, c := range access {
		if c.Role == "session" {
			continue
		}
		out = append(out, v1.ManagedCredential{CreatedIndex: c.CreatedIndex, CredentialID: c.CredentialID, Name: c.Name, Kind: c.Role, CreatedUnix: c.CreatedUnix, ExpiresUnix: c.ExpiresUnix, Status: credentialState(c.CreatedUnix, c.ExpiresUnix, c.RevokedIndex), RequestID: c.RequestID, Current: c.CredentialID == current})
	}
	for _, c := range credentials {
		t := byTenant[c.TenantID]
		modules := t.Modules
		if c.Modules != nil {
			modules = []string{}
			for _, m := range c.Modules {
				if t.AllowsModule(m) {
					modules = append(modules, m)
				}
			}
		}
		permission := c.Permission
		if permission == "" {
			permission = "readwrite"
		}
		out = append(out, v1.ManagedCredential{CreatedIndex: c.CreatedIndex, CredentialID: c.CredentialID, Name: c.Name, Kind: "tenant", TenantID: c.TenantID, TenantName: t.Name, Modules: modules, Permission: permission, CreatedUnix: c.CreatedUnix, ExpiresUnix: c.ExpiresUnix, Status: credentialState(c.CreatedUnix, c.ExpiresUnix, c.RevokedIndex), RequestID: c.RequestID})
	}
	sort.Slice(out, func(i, j int) bool {
		return CredentialListKey(out[i]) < CredentialListKey(out[j])
	})
	return out, nil
}
func (s *Operations) Issue(ctx context.Context, in v1.CredentialMutation) (v1.TenantToken, error) {
	if rootstate.ValidateRequestID(in.RequestID) != nil || strings.TrimSpace(in.Name) == "" || rootstate.ValidateCredentialName(in.Name) != nil || !validCredentialTTL(in.TTLSeconds, in.NeverExpires) || (in.Kind != "tenant" && in.Kind != "admin" && in.Kind != "monitor") || (in.Kind != "tenant" && in.TenantID != "") {
		return v1.TenantToken{}, tenantError(400, "invalid_request", "凭证名称、类型、租户或有效期无效。")
	}
	if in.Modules != nil && len(in.Modules) == 0 {
		return v1.TenantToken{}, tenantError(400, "invalid_request", "请至少选择一个模块。")
	}
	if (in.Permission != "" && in.Permission != "readwrite" && in.Permission != "readonly") || (in.Kind != "tenant" && (in.Permission != "" || in.Modules != nil)) {
		return v1.TenantToken{}, tenantError(400, "invalid_request", "凭证权限无效。")
	}
	if _, err := s.Barrier(ctx); err != nil {
		return v1.TenantToken{}, err
	}
	if _, found, err := s.Root.Result(in.RequestID); err != nil {
		return v1.TenantToken{}, err
	} else if found {
		return v1.TenantToken{}, tenantError(409, "token_not_recoverable", "此请求已处理，Token 无法再次取回。请按请求标识核对并撤销不用的凭证。")
	}
	var token, id string
	var expires int64
	command := rootstate.Command{Version: rootstate.FormatVersion, RequestID: in.RequestID}
	if in.Kind == "tenant" {
		if rootstate.ValidateTenantID(in.TenantID) != nil {
			return v1.TenantToken{}, tenantError(400, "invalid_request", "租户标识无效。")
		}
		if _, found, err := s.Root.Tenant(in.TenantID); err != nil {
			return v1.TenantToken{}, err
		} else if !found {
			return v1.TenantToken{}, tenantError(404, "not_found", "租户不存在。")
		}
		t, c, err := auth.Generate(in.TenantID, time.Now(), time.Duration(in.TTLSeconds)*time.Second)
		if err != nil {
			return v1.TenantToken{}, err
		}
		c.Name = in.Name
		c.RequestID = in.RequestID
		c.Modules = in.Modules
		c.Permission = in.Permission
		token = t
		id = c.CredentialID
		expires = c.ExpiresUnix
		command.Type = rootstate.CreateTenantCredential
		command.TenantCredential = &c
	} else {
		t, c, err := auth.GenerateAccess(in.Kind, "", time.Now(), time.Duration(in.TTLSeconds)*time.Second)
		if err != nil {
			return v1.TenantToken{}, err
		}
		c.Name = in.Name
		c.RequestID = in.RequestID
		token = t
		id = c.CredentialID
		expires = c.ExpiresUnix
		command.Type = rootstate.CreateAccessCredential
		command.AccessCredential = &c
	}
	if err := s.commit(ctx, command); err != nil {
		return v1.TenantToken{}, err
	}
	return v1.TenantToken{CredentialID: id, Token: token, ExpiresUnix: expires}, nil
}
func (s *Operations) Revoke(ctx context.Context, id, request, current string) error {
	if rootstate.ValidateRequestID(request) != nil {
		return tenantError(400, "invalid_request", "请求标识无效。")
	}
	all, err := s.Credentials(ctx, current)
	if err != nil {
		return err
	}
	for _, c := range all {
		if c.CredentialID != id {
			continue
		}
		if c.Current {
			return tenantError(409, "current_credential", "不能撤销当前登录所用的管理凭证，请使用另一管理凭证登录后操作。")
		}
		if c.Status == "revoked" {
			return nil
		}
		command := rootstate.Command{Version: rootstate.FormatVersion, RequestID: request}
		r := &rootstate.TenantCredentialRevocation{CredentialID: id}
		if c.Kind == "tenant" {
			command.Type = rootstate.RevokeTenantCredential
			command.TenantRevocation = r
		} else {
			command.Type = rootstate.RevokeAccessCredential
			command.AccessRevocation = r
		}
		return s.commit(ctx, command)
	}
	return tenantError(404, "not_found", "凭证不存在。")
}
func repositoryDTO(r repository.Repository, def string, jobs []rootstate.BackupJob) v1.ManagedRepository {
	out := v1.ManagedRepository{CreatedIndex: r.CreatedIndex, Name: r.Name, Type: string(r.Type), Path: r.Path, Bucket: r.Bucket, Prefix: r.Prefix, Endpoint: r.Endpoint, Region: r.Region, PathStyle: r.PathStyle, CredentialsConfigured: len(r.EncryptedCredentials) > 0, Revision: r.Revision(), Default: r.Name == def}
	for _, j := range jobs {
		if j.Repository == r.Name && j.State == rootstate.BackupJobCompleted {
			out.CompletedBackups++
			out.LatestCreatedUnix = max(out.LatestCreatedUnix, j.CreatedUnix)
		}
	}
	return out
}
func (s *Operations) Repositories(ctx context.Context) ([]v1.ManagedRepository, error) {
	if _, err := s.Barrier(ctx); err != nil {
		return nil, err
	}
	rs, err := s.Root.Repositories()
	if err != nil {
		return nil, err
	}
	def, _, err := s.Root.DefaultRepository()
	if err != nil {
		return nil, err
	}
	jobs, err := s.Root.BackupJobs()
	if err != nil {
		return nil, err
	}
	out := []v1.ManagedRepository{}
	for _, r := range rs {
		out = append(out, repositoryDTO(r, def.Name, jobs))
	}
	sort.Slice(out, func(i, j int) bool {
		return CreationKey(out[i].CreatedIndex, out[i].Name) < CreationKey(out[j].CreatedIndex, out[j].Name)
	})
	return out, nil
}
func (s *Operations) SaveRepository(ctx context.Context, in v1.RepositoryMutation, create bool) error {
	if rootstate.ValidateRequestID(in.RequestID) != nil || (!create && len(in.Revision) != 64) || (create && in.Revision != "") {
		return tenantError(400, "invalid_request", "请求标识或仓库版本无效。")
	}
	if in.Type == "" {
		in.Type = "file"
	}
	r := repository.Repository{Name: in.Name, Type: repository.Type(in.Type), Path: in.Path, Bucket: in.Bucket, Prefix: in.Prefix, Endpoint: in.Endpoint, Region: in.Region, PathStyle: in.PathStyle}
	if r.Validate() != nil {
		return tenantError(400, "invalid_request", "仓库参数无效，请检查类型、目录与 S3 配置。")
	}
	if _, err := s.Barrier(ctx); err != nil {
		return err
	}
	old, found, err := s.Root.Repository(in.Name)
	if err != nil {
		return err
	}
	if !create && !found {
		return tenantError(404, "not_found", "仓库不存在。")
	}
	if !create && r.Type != old.Type {
		return tenantError(409, "conflict", "不能修改仓库类型，请创建新仓库。")
	}
	hasSecret := in.AccessKey != "" || in.SecretKey != "" || in.SessionToken != ""
	if hasSecret {
		if !r.IsS3() || in.AccessKey == "" || in.SecretKey == "" {
			return tenantError(400, "invalid_request", "S3 访问密钥必须成对填写。")
		}
		// Encryption uses a fresh nonce. An uncertain encrypted mutation must be
		// reconciled by request ID instead of silently minting a second envelope.
		if _, seen, err := s.Root.Result(in.RequestID); err != nil {
			return err
		} else if seen {
			return tenantError(409, "already_processed", "此请求已处理，请刷新仓库核对结果。")
		}
		key, err := s.RepositoryKey()
		if err != nil {
			return tenantError(503, "key_unavailable", "节点尚未取得集群仓库密钥，请稍后重试。")
		}
		r, err = repository.SealS3Credentials(r, key, 1, repository.S3Credentials{AccessKey: in.AccessKey, SecretKey: in.SecretKey, SessionToken: in.SessionToken})
		if err != nil {
			return tenantError(400, "invalid_request", "S3 凭据格式无效。")
		}
	} else if r.IsS3() && (create || len(old.EncryptedCredentials) == 0) {
		return tenantError(400, "invalid_request", "请配置 S3 访问凭据。")
	}
	kind := rootstate.CreateRepository
	if !create {
		kind = rootstate.UpdateRepository
	}
	return s.commit(ctx, rootstate.Command{Version: rootstate.FormatVersion, RequestID: in.RequestID, Type: kind, Repository: &r, RepositoryIfMatch: in.Revision})
}
func (s *Operations) Default(ctx context.Context, name, request string) error {
	if rootstate.ValidateRequestID(request) != nil || repository.ValidateName(name) != nil {
		return tenantError(400, "invalid_request", "请求或仓库标识无效。")
	}
	return s.commit(ctx, rootstate.Command{Version: rootstate.FormatVersion, RequestID: request, Type: rootstate.SetDefaultRepository, RepositoryDefault: &rootstate.RepositoryDefault{Name: name}})
}
func (s *Operations) Backups(ctx context.Context) ([]v1.ManagedBackup, error) {
	if _, err := s.Barrier(ctx); err != nil {
		return nil, err
	}
	jobs, err := s.Root.BackupJobs()
	if err != nil {
		return nil, err
	}
	view, err := s.Root.RootRoutingView(4096)
	if err != nil {
		return nil, err
	}
	nodes := map[string]rootstate.Node{}
	for _, n := range view.Nodes {
		nodes[n.NodeID] = n
	}
	repos, err := s.Root.Repositories()
	if err != nil {
		return nil, err
	}
	types := map[string]string{}
	for _, r := range repos {
		types[r.Name] = string(r.Type)
	}
	out := []v1.ManagedBackup{}
	for _, j := range jobs {
		v := v1.ManagedBackup{JobID: j.JobID, Repository: j.Repository, RepositoryType: types[j.Repository], ClusterID: j.ClusterID, Epoch: j.Epoch, CreatedIndex: j.CreatedIndex, CreatedUnix: j.CreatedUnix, State: j.State, TotalNodes: len(j.Workers), Workers: []v1.ManagedBackupWorker{}}
		if j.LastError != "" {
			v.Error = "备份未完成，请检查各节点日志与仓库配置。"
		}
		for _, w := range j.Workers {
			n := nodes[w.NodeID]
			worker := v1.ManagedBackupWorker{NodeID: w.NodeID, Name: n.Name, Region: n.RegionID, State: w.State, Bytes: w.Bytes, Objects: w.Objects}
			if w.Error != "" {
				worker.Error = "节点备份失败，请检查该节点日志。"
			}
			v.Workers = append(v.Workers, worker)
			v.Bytes += w.Bytes
			v.Objects += w.Objects
			if w.State == rootstate.BackupWorkerCompleted {
				v.CompletedNodes++
			}
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		return CreationKey(out[i].CreatedIndex, out[i].JobID) < CreationKey(out[j].CreatedIndex, out[j].JobID)
	})
	return out, nil
}
func (s *Operations) Run(ctx context.Context, in v1.BackupMutation) (string, error) {
	if rootstate.ValidateRequestID(in.RequestID) != nil || repository.ValidateName(in.Repository) != nil {
		return "", tenantError(400, "invalid_request", "请求标识或仓库无效。")
	}
	id := "backup-" + in.RequestID
	if rootstate.ValidateRequestID(id) != nil {
		return "", tenantError(400, "invalid_request", "请求标识过长。")
	}
	if _, err := s.Barrier(ctx); err != nil {
		return "", err
	}
	job := rootstate.BackupJob{JobID: id, Repository: in.Repository, ClusterID: s.ClusterID, CreatedUnix: time.Now().Unix()}
	// Preserve the original payload on exact retries, including its timestamp.
	if old, found, err := s.Root.BackupJob(id); err != nil {
		return "", err
	} else if found {
		result, committed, lookupErr := s.Root.Result(in.RequestID)
		if lookupErr != nil {
			return "", lookupErr
		}
		if old.Repository != in.Repository || !committed || result.Index != old.CreatedIndex || result.Error != "" {
			return "", tenantError(409, "request_id_reused", "请求标识已用于其他操作。")
		}
		return id, nil
	}
	err := s.commit(ctx, rootstate.Command{Version: rootstate.FormatVersion, RequestID: in.RequestID, Type: rootstate.CreateBackupJob, BackupJob: &job})
	return id, err
}
func (s *Operations) Preparation(ctx context.Context, id string) (v1.RestorePreparation, error) {
	all, err := s.Backups(ctx)
	if err != nil {
		return v1.RestorePreparation{}, err
	}
	for _, j := range all {
		if j.JobID != id {
			continue
		}
		if j.State != "completed" {
			return v1.RestorePreparation{}, tenantError(409, "backup_incomplete", "只能使用已完成封存的备份。")
		}
		rs, err := s.Repositories(ctx)
		if err != nil {
			return v1.RestorePreparation{}, err
		}
		for _, r := range rs {
			if r.Name == j.Repository {
				return v1.RestorePreparation{Backup: j, Repository: r, Target: "new-empty-cluster", Steps: []string{"准备全新空集群与目标节点映射，保留原集群。", "取得备份仓库与访问凭据；文件目录仓库需取得各原节点的备份目录。", "使用恢复工具校验恢复点清单和所有对象，再恢复底座状态与数据。", "启动新集群，等待数据修复与再平衡完成后验收业务数据。"}}, nil
			}
		}
		return v1.RestorePreparation{}, tenantError(409, "repository_missing", "原备份仓库配置已不存在。")
	}
	return v1.RestorePreparation{}, tenantError(404, "not_found", "备份不存在。")
}
