// Package nodeadminrpc exposes the small, Node-authenticated local maintenance
// surface. It is not a Tenant API and does not infer removal from health.
package nodeadminrpc

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	methodRemoveNode            = "/dubi.internal.nodeadmin.v1.NodeAdmin/RemoveNode"
	methodRemoveNodeByName      = "/dubi.internal.nodeadmin.v1.NodeAdmin/RemoveNodeByName"
	methodRenameNode            = "/dubi.internal.nodeadmin.v1.NodeAdmin/RenameNode"
	methodSignerWrappingKey     = "/dubi.internal.nodeadmin.v1.NodeAdmin/SignerWrappingKey"
	methodTransferSigner        = "/dubi.internal.nodeadmin.v1.NodeAdmin/TransferSigner"
	methodTransferRepositoryKey = "/dubi.internal.nodeadmin.v1.NodeAdmin/TransferRepositoryKey"
)

type RemoveFunc func(context.Context, uint64) error
type RemoveByNameFunc func(context.Context, string) (uint64, error)

// RemoveRouter is used by a node that received a management request while it
// is not the current Root leader. The router returns handled=true when it
// forwarded the request (successfully or with an error). Forwarded requests
// skip routing once and are executed only on the selected candidate.
type RemoveRouter func(context.Context, uint64) (handled bool, err error)
type RemoveByNameRouter func(context.Context, string) (handled bool, target uint64, err error)
type RenameFunc func(context.Context, string) error
type SignerWrappingKeyFunc func(context.Context) ([]byte, error)
type TransferSignerFunc func(context.Context, uint64, uint64, []byte) error
type TransferRepositoryKeyFunc func(context.Context, uint64, []byte) error

type Server struct {
	identity              identity.Identity
	authorize             raftrpc.AuthorizePeer
	remove                RemoveFunc
	removeByName          RemoveByNameFunc
	routeRemove           RemoveRouter
	routeByName           RemoveByNameRouter
	rename                RenameFunc
	wrappingKey           SignerWrappingKeyFunc
	transfer              TransferSignerFunc
	transferRepositoryKey TransferRepositoryKeyFunc
}

func (s *Server) SetRenameFunc(rename RenameFunc) { s.rename = rename }

func (s *Server) SetRemoveByNameFunc(remove RemoveByNameFunc) { s.removeByName = remove }

func (s *Server) SetRemoveRouter(router RemoveRouter) { s.routeRemove = router }

func (s *Server) SetRemoveByNameRouter(router RemoveByNameRouter) { s.routeByName = router }

func (s *Server) SetTransferSignerFunc(transfer TransferSignerFunc) { s.transfer = transfer }

func (s *Server) SetTransferRepositoryKeyFunc(transfer TransferRepositoryKeyFunc) {
	s.transferRepositoryKey = transfer
}

func (s *Server) SetSignerWrappingKeyFunc(key SignerWrappingKeyFunc) { s.wrappingKey = key }

func NewServer(local identity.Identity, authorize raftrpc.AuthorizePeer, remove RemoveFunc) (*Server, error) {
	if local.ClusterID == "" || local.NodeID == "" || local.RaftID == 0 || authorize == nil || remove == nil {
		return nil, errors.New("Node admin RPC requires local identity, peer authorizer and remove function")
	}
	return &Server{identity: local, authorize: authorize, remove: remove}, nil
}

func RegisterServer(registrar grpc.ServiceRegistrar, server *Server) {
	registrar.RegisterService(&serviceDesc, server)
}

func (s *Server) RemoveNode(ctx context.Context, request *structpb.Struct) (*structpb.Struct, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "Node removal request is required")
	}
	clusterID, err := stringField(request, "cluster_id")
	if err != nil || clusterID != s.identity.ClusterID {
		return nil, status.Error(codes.PermissionDenied, "Node removal request belongs to another Cluster")
	}
	from, err := uintField(request, "from_raft_id")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "from_raft_id is required")
	}
	target, err := uintField(request, "target_raft_id")
	if err != nil || target == 0 {
		return nil, status.Error(codes.InvalidArgument, "target_raft_id is required")
	}
	if err := s.authorizeLocal(ctx, from); err != nil {
		return nil, err
	}
	forwarded, err := optionalBoolField(request, "forwarded")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !forwarded && s.routeRemove != nil {
		if handled, err := s.routeRemove(ctx, target); handled {
			if err != nil {
				return nil, status.Errorf(codes.FailedPrecondition, "Node removal did not complete: %v", err)
			}
			return structpb.NewStruct(map[string]any{"cluster_id": s.identity.ClusterID, "target_raft_id": strconv.FormatUint(target, 10), "completed": true})
		}
	}
	if err := s.remove(ctx, target); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Errorf(codes.FailedPrecondition, "Node removal did not complete: %v", err)
	}
	return structpb.NewStruct(map[string]any{"cluster_id": s.identity.ClusterID, "target_raft_id": strconv.FormatUint(target, 10), "completed": true})
}

func (s *Server) RemoveNodeByName(ctx context.Context, request *structpb.Struct) (*structpb.Struct, error) {
	if s.removeByName == nil {
		return nil, status.Error(codes.Unimplemented, "Node name removal is unavailable")
	}
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "Node removal request is required")
	}
	clusterID, err := stringField(request, "cluster_id")
	if err != nil || clusterID != s.identity.ClusterID {
		return nil, status.Error(codes.PermissionDenied, "Node removal request belongs to another Cluster")
	}
	from, err := uintField(request, "from_raft_id")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "from_raft_id is required")
	}
	name, err := requiredStringField(request, "target_name")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.authorizeLocal(ctx, from); err != nil {
		return nil, err
	}
	forwarded, err := optionalBoolField(request, "forwarded")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !forwarded && s.routeByName != nil {
		if handled, target, err := s.routeByName(ctx, name); handled {
			if err != nil {
				return nil, status.Errorf(codes.FailedPrecondition, "Node removal did not complete: %v", err)
			}
			return structpb.NewStruct(map[string]any{"cluster_id": s.identity.ClusterID, "target_name": name, "target_raft_id": strconv.FormatUint(target, 10), "completed": true})
		}
	}
	target, err := s.removeByName(ctx, name)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Errorf(codes.FailedPrecondition, "Node removal did not complete: %v", err)
	}
	return structpb.NewStruct(map[string]any{"cluster_id": s.identity.ClusterID, "target_name": name, "target_raft_id": strconv.FormatUint(target, 10), "completed": true})
}

func (s *Server) RenameNode(ctx context.Context, request *structpb.Struct) (*structpb.Struct, error) {
	if s.rename == nil {
		return nil, status.Error(codes.Unimplemented, "Node rename is unavailable")
	}
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "Node rename request is required")
	}
	clusterID, err := stringField(request, "cluster_id")
	if err != nil || clusterID != s.identity.ClusterID {
		return nil, status.Error(codes.PermissionDenied, "Node rename request belongs to another Cluster")
	}
	from, err := uintField(request, "from_raft_id")
	if err != nil || from != s.identity.RaftID {
		return nil, status.Error(codes.PermissionDenied, "Node rename must be requested through the local Node")
	}
	name, err := optionalStringField(request, "name")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.authorizeLocal(ctx, from); err != nil {
		return nil, err
	}
	if err := s.rename(ctx, name); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Errorf(codes.FailedPrecondition, "Node rename did not complete: %v", err)
	}
	return structpb.NewStruct(map[string]any{"cluster_id": s.identity.ClusterID, "name": name, "completed": true})
}

func (s *Server) TransferSigner(ctx context.Context, request *structpb.Struct) (*structpb.Struct, error) {
	if s.transfer == nil {
		return nil, status.Error(codes.Unimplemented, "signer transfer is unavailable")
	}
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "signer transfer request is required")
	}
	clusterID, err := stringField(request, "cluster_id")
	if err != nil || clusterID != s.identity.ClusterID {
		return nil, status.Error(codes.PermissionDenied, "signer transfer belongs to another Cluster")
	}
	from, err := uintField(request, "from_raft_id")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	epoch, err := uintField(request, "authority_epoch")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	encoded, err := stringField(request, "envelope")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	envelope, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(envelope) == 0 {
		return nil, status.Error(codes.InvalidArgument, "envelope is invalid")
	}
	if err := s.authorizePeer(ctx, from); err != nil {
		return nil, err
	}
	if err := s.transfer(ctx, from, epoch, envelope); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Errorf(codes.FailedPrecondition, "signer transfer did not complete: %v", err)
	}
	return structpb.NewStruct(map[string]any{"cluster_id": s.identity.ClusterID, "authority_epoch": strconv.FormatUint(epoch, 10), "completed": true})
}

func (s *Server) SignerWrappingKey(ctx context.Context, request *structpb.Struct) (*structpb.Struct, error) {
	if s.wrappingKey == nil {
		return nil, status.Error(codes.Unimplemented, "signer wrapping key is unavailable")
	}
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "signer wrapping key request is required")
	}
	clusterID, err := stringField(request, "cluster_id")
	if err != nil || clusterID != s.identity.ClusterID {
		return nil, status.Error(codes.PermissionDenied, "signer wrapping key belongs to another Cluster")
	}
	from, err := uintField(request, "from_raft_id")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.authorizePeer(ctx, from); err != nil {
		return nil, err
	}
	key, err := s.wrappingKey(ctx)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "read signer wrapping key: %v", err)
	}
	return structpb.NewStruct(map[string]any{"cluster_id": s.identity.ClusterID, "raft_id": strconv.FormatUint(s.identity.RaftID, 10), "key": base64.RawURLEncoding.EncodeToString(key)})
}

func (s *Server) TransferRepositoryKey(ctx context.Context, request *structpb.Struct) (*structpb.Struct, error) {
	if s.transferRepositoryKey == nil {
		return nil, status.Error(codes.Unimplemented, "Repository key transfer is unavailable")
	}
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "Repository key transfer request is required")
	}
	clusterID, err := stringField(request, "cluster_id")
	if err != nil || clusterID != s.identity.ClusterID {
		return nil, status.Error(codes.PermissionDenied, "Repository key transfer belongs to another Cluster")
	}
	from, err := uintField(request, "from_raft_id")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "from_raft_id is required")
	}
	encoded, err := stringField(request, "key")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	key, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(key) != 32 {
		return nil, status.Error(codes.InvalidArgument, "Repository key is invalid")
	}
	if err := s.authorizePeer(ctx, from); err != nil {
		return nil, err
	}
	if err := s.transferRepositoryKey(ctx, from, key); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Errorf(codes.FailedPrecondition, "Repository key transfer did not complete: %v", err)
	}
	return structpb.NewStruct(map[string]any{"cluster_id": s.identity.ClusterID, "completed": true})
}

func (s *Server) authorizeLocal(ctx context.Context, raftID uint64) error {
	return s.authorizePeer(ctx, raftID)
}

func (s *Server) authorizePeer(ctx context.Context, raftID uint64) error {
	remote, ok := peer.FromContext(ctx)
	if !ok || remote == nil {
		return status.Error(codes.Unauthenticated, "Node TLS identity is required")
	}
	info, ok := remote.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.VerifiedChains) == 0 || len(info.State.PeerCertificates) == 0 {
		return status.Error(codes.Unauthenticated, "verified Node TLS identity is required")
	}
	parsed, err := identity.ParseNodeCertificate(info.State.PeerCertificates[0])
	if err != nil || parsed.ClusterID != s.identity.ClusterID || parsed.RaftID != raftID {
		return status.Error(codes.PermissionDenied, "Node TLS identity does not match the request source")
	}
	if _, err := identity.VerifiedPeerCertificate(ctx, time.Now().UTC()); err != nil {
		return status.Error(codes.PermissionDenied, "Node TLS certificate is not currently valid")
	}
	if err := s.authorize(ctx, raftID); err != nil {
		return status.Errorf(codes.PermissionDenied, "transfer source is not authorized: %v", err)
	}
	return nil
}

func RemoveNode(ctx context.Context, conn grpc.ClientConnInterface, clusterID string, from, target uint64) error {
	return removeNode(ctx, conn, clusterID, from, target, false)
}

// RemoveNodeForwarded is used internally after a follower has routed a
// request to a Root candidate. It is intentionally not exposed by the CLI.
func RemoveNodeForwarded(ctx context.Context, conn grpc.ClientConnInterface, clusterID string, from, target uint64) error {
	return removeNode(ctx, conn, clusterID, from, target, true)
}

func removeNode(ctx context.Context, conn grpc.ClientConnInterface, clusterID string, from, target uint64, forwarded bool) error {
	request, err := structpb.NewStruct(map[string]any{"cluster_id": clusterID, "from_raft_id": strconv.FormatUint(from, 10), "target_raft_id": strconv.FormatUint(target, 10), "forwarded": forwarded})
	if err != nil {
		return err
	}
	response := new(structpb.Struct)
	return conn.Invoke(ctx, methodRemoveNode, request, response)
}

func RemoveNodeByName(ctx context.Context, conn grpc.ClientConnInterface, clusterID string, from uint64, name string) (uint64, error) {
	return removeNodeByName(ctx, conn, clusterID, from, name, false)
}

// RemoveNodeByNameForwarded is the name form of RemoveNodeForwarded.
func RemoveNodeByNameForwarded(ctx context.Context, conn grpc.ClientConnInterface, clusterID string, from uint64, name string) (uint64, error) {
	return removeNodeByName(ctx, conn, clusterID, from, name, true)
}

func removeNodeByName(ctx context.Context, conn grpc.ClientConnInterface, clusterID string, from uint64, name string, forwarded bool) (uint64, error) {
	request, err := structpb.NewStruct(map[string]any{"cluster_id": clusterID, "from_raft_id": strconv.FormatUint(from, 10), "target_name": name, "forwarded": forwarded})
	if err != nil {
		return 0, err
	}
	response := new(structpb.Struct)
	if err := conn.Invoke(ctx, methodRemoveNodeByName, request, response); err != nil {
		return 0, err
	}
	return uintField(response, "target_raft_id")
}

func RenameNode(ctx context.Context, conn grpc.ClientConnInterface, clusterID string, from uint64, name string) error {
	request, err := structpb.NewStruct(map[string]any{"cluster_id": clusterID, "from_raft_id": strconv.FormatUint(from, 10), "name": name})
	if err != nil {
		return err
	}
	response := new(structpb.Struct)
	return conn.Invoke(ctx, methodRenameNode, request, response)
}

func TransferSigner(ctx context.Context, conn grpc.ClientConnInterface, clusterID string, from uint64, epoch uint64, envelope []byte) error {
	if epoch == 0 || len(envelope) == 0 {
		return errors.New("signer transfer requires an authority epoch and envelope")
	}
	request, err := structpb.NewStruct(map[string]any{"cluster_id": clusterID, "from_raft_id": strconv.FormatUint(from, 10), "authority_epoch": strconv.FormatUint(epoch, 10), "envelope": base64.RawURLEncoding.EncodeToString(envelope)})
	if err != nil {
		return err
	}
	response := new(structpb.Struct)
	return conn.Invoke(ctx, methodTransferSigner, request, response)
}

func TransferRepositoryKey(ctx context.Context, conn grpc.ClientConnInterface, clusterID string, from uint64, key []byte) error {
	if len(key) != 32 {
		return errors.New("Repository key transfer requires a 32-byte key")
	}
	request, err := structpb.NewStruct(map[string]any{"cluster_id": clusterID, "from_raft_id": strconv.FormatUint(from, 10), "key": base64.RawURLEncoding.EncodeToString(key)})
	if err != nil {
		return err
	}
	response := new(structpb.Struct)
	return conn.Invoke(ctx, methodTransferRepositoryKey, request, response)
}

func SignerWrappingKey(ctx context.Context, conn grpc.ClientConnInterface, clusterID string, from uint64) ([]byte, error) {
	request, err := structpb.NewStruct(map[string]any{"cluster_id": clusterID, "from_raft_id": strconv.FormatUint(from, 10)})
	if err != nil {
		return nil, err
	}
	response := new(structpb.Struct)
	if err := conn.Invoke(ctx, methodSignerWrappingKey, request, response); err != nil {
		return nil, err
	}
	encoded, err := stringField(response, "key")
	if err != nil {
		return nil, err
	}
	key, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(key) != 32 {
		return nil, errors.New("signer wrapping key response is invalid")
	}
	return key, nil
}

func stringField(request *structpb.Struct, name string) (string, error) {
	value, ok := request.Fields[name]
	if !ok || value.GetStringValue() == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value.GetStringValue(), nil
}

func requiredStringField(request *structpb.Struct, name string) (string, error) {
	value, err := optionalStringField(request, name)
	if err != nil || value == "" {
		if err != nil {
			return "", err
		}
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}

func optionalStringField(request *structpb.Struct, name string) (string, error) {
	value, ok := request.Fields[name]
	if !ok {
		return "", nil
	}
	if value == nil {
		return "", fmt.Errorf("%s is invalid", name)
	}
	if _, ok := value.Kind.(*structpb.Value_StringValue); !ok {
		return "", fmt.Errorf("%s is invalid", name)
	}
	return value.GetStringValue(), nil
}

func optionalBoolField(request *structpb.Struct, name string) (bool, error) {
	value, ok := request.Fields[name]
	if !ok {
		return false, nil
	}
	if value == nil {
		return false, fmt.Errorf("%s is invalid", name)
	}
	if _, ok := value.Kind.(*structpb.Value_BoolValue); !ok {
		return false, fmt.Errorf("%s is invalid", name)
	}
	return value.GetBoolValue(), nil
}

func uintField(request *structpb.Struct, name string) (uint64, error) {
	value, ok := request.Fields[name]
	if !ok || value.GetStringValue() == "" {
		return 0, fmt.Errorf("%s is invalid", name)
	}
	parsed, err := strconv.ParseUint(value.GetStringValue(), 10, 64)
	if err != nil || parsed == 0 {
		return 0, fmt.Errorf("%s is invalid", name)
	}
	return parsed, nil
}

var serviceDesc = grpc.ServiceDesc{
	ServiceName: "dubi.internal.nodeadmin.v1.NodeAdmin",
	HandlerType: (*interface {
		RemoveNode(context.Context, *structpb.Struct) (*structpb.Struct, error)
		RemoveNodeByName(context.Context, *structpb.Struct) (*structpb.Struct, error)
		RenameNode(context.Context, *structpb.Struct) (*structpb.Struct, error)
		SignerWrappingKey(context.Context, *structpb.Struct) (*structpb.Struct, error)
		TransferSigner(context.Context, *structpb.Struct) (*structpb.Struct, error)
		TransferRepositoryKey(context.Context, *structpb.Struct) (*structpb.Struct, error)
	})(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "RemoveNode",
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			request := new(structpb.Struct)
			if err := dec(request); err != nil {
				return nil, err
			}
			invoke := func(ctx context.Context, req any) (any, error) {
				return srv.(*Server).RemoveNode(ctx, req.(*structpb.Struct))
			}
			if interceptor == nil {
				return invoke(ctx, request)
			}
			return interceptor(ctx, request, &grpc.UnaryServerInfo{Server: srv, FullMethod: methodRemoveNode}, invoke)
		},
	}, {
		MethodName: "RemoveNodeByName",
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			request := new(structpb.Struct)
			if err := dec(request); err != nil {
				return nil, err
			}
			invoke := func(ctx context.Context, req any) (any, error) {
				return srv.(*Server).RemoveNodeByName(ctx, req.(*structpb.Struct))
			}
			if interceptor == nil {
				return invoke(ctx, request)
			}
			return interceptor(ctx, request, &grpc.UnaryServerInfo{Server: srv, FullMethod: methodRemoveNodeByName}, invoke)
		},
	}, {
		MethodName: "RenameNode",
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			request := new(structpb.Struct)
			if err := dec(request); err != nil {
				return nil, err
			}
			invoke := func(ctx context.Context, req any) (any, error) {
				return srv.(*Server).RenameNode(ctx, req.(*structpb.Struct))
			}
			if interceptor == nil {
				return invoke(ctx, request)
			}
			return interceptor(ctx, request, &grpc.UnaryServerInfo{Server: srv, FullMethod: methodRenameNode}, invoke)
		},
	}, {
		MethodName: "SignerWrappingKey",
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			request := new(structpb.Struct)
			if err := dec(request); err != nil {
				return nil, err
			}
			invoke := func(ctx context.Context, req any) (any, error) {
				return srv.(*Server).SignerWrappingKey(ctx, req.(*structpb.Struct))
			}
			if interceptor == nil {
				return invoke(ctx, request)
			}
			return interceptor(ctx, request, &grpc.UnaryServerInfo{Server: srv, FullMethod: methodSignerWrappingKey}, invoke)
		},
	}, {
		MethodName: "TransferSigner",
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			request := new(structpb.Struct)
			if err := dec(request); err != nil {
				return nil, err
			}
			invoke := func(ctx context.Context, req any) (any, error) {
				return srv.(*Server).TransferSigner(ctx, req.(*structpb.Struct))
			}
			if interceptor == nil {
				return invoke(ctx, request)
			}
			return interceptor(ctx, request, &grpc.UnaryServerInfo{Server: srv, FullMethod: methodTransferSigner}, invoke)
		},
	}, {
		MethodName: "TransferRepositoryKey",
		Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
			in := new(structpb.Struct)
			if err := dec(in); err != nil {
				return nil, err
			}
			if interceptor == nil {
				return srv.(*Server).TransferRepositoryKey(ctx, in)
			}
			info := &grpc.UnaryServerInfo{Server: srv, FullMethod: methodTransferRepositoryKey}
			handler := func(ctx context.Context, req interface{}) (interface{}, error) {
				return srv.(*Server).TransferRepositoryKey(ctx, req.(*structpb.Struct))
			}
			return interceptor(ctx, in, info, handler)
		},
	},
	},
}
