package server

import (
	"context"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestRecordRejectsClientStorageIdentityBeforeAdmission(t *testing.T) {
	// Legacy Dataset fields and invented Module/Engine fields must not silently
	// select or alias another scope. Reject before touching even a nil Runtime.
	service := &dataService{}
	for _, field := range []protowire.Number{4, 5, 90, 91} {
		unknown := protowire.AppendString(protowire.AppendTag(nil, field, protowire.BytesType), "session")
		put := &datawire.PutRecordRequest{RequestId: "spoof", TenantId: "tenant", Key: []byte("key")}
		put.ProtoReflect().SetUnknown(unknown)
		if _, err := service.PutRecord(context.Background(), put); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("put accepted field %d: %v", field, err)
		}
		get := &datawire.GetRecordRequest{TenantId: "tenant", Key: []byte("key")}
		get.ProtoReflect().SetUnknown(unknown)
		if _, err := service.GetRecord(context.Background(), get); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("get accepted field %d: %v", field, err)
		}
	}
}
