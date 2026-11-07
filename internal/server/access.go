package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Administrative bootstrap/recovery uses the existing Node-authenticated RPC.
// No plaintext access token crosses this internal mutation endpoint.
func (s *dataService) MutateAccessCredential(ctx context.Context, r *datawire.AccessCredentialRequest) (*datawire.AccessCredentialResponse, error) {
	if r == nil || proto.Size(r) > 8192 || rootstate.ValidateRequestID(r.RequestId) != nil || len(r.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid access mutation")
	}
	ctx, release, err := s.begin(ctx, r.ClusterId, r.FromRaftId, proto.Size(r))
	if err != nil {
		return nil, err
	}
	defer release()
	c := rootstate.Command{Version: rootstate.FormatVersion, RequestID: r.RequestId}
	if r.RevokeId != "" && len(r.CredentialJson) == 0 {
		c.Type = rootstate.RevokeAccessCredential
		c.AccessRevocation = &rootstate.TenantCredentialRevocation{CredentialID: r.RevokeId}
	} else if r.RevokeId == "" && len(r.CredentialJson) > 0 {
		var v rootstate.AccessCredential
		d := json.NewDecoder(bytes.NewReader(r.CredentialJson))
		d.DisallowUnknownFields()
		if d.Decode(&v) != nil || d.Decode(new(any)) != io.EOF || v.Validate() != nil || v.ParentID != "" || v.RevokedIndex != 0 || v.CreatedUnix > time.Now().Add(time.Minute).Unix() || (v.ExpiresUnix != 0 && v.ExpiresUnix <= time.Now().Unix()) {
			return nil, status.Error(codes.InvalidArgument, "invalid access credential")
		}
		c.Type = rootstate.CreateAccessCredential
		c.AccessCredential = &v
	} else {
		return nil, status.Error(codes.InvalidArgument, "exactly one mutation required")
	}
	host, err := newRootControlHost(s.runtime.Host, s.client, s.runtime.Identity.RaftID)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "Root routing unavailable")
	}
	if err := s.rootCommandWithSubmit(ctx, c, host.Submit); err != nil {
		return nil, status.Error(codes.Unavailable, "access mutation unconfirmed; retry same request or revoke credential")
	}
	return &datawire.AccessCredentialResponse{RootIndex: s.runtime.Root.AppliedIndex()}, nil
}
