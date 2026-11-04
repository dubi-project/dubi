package raftrpc

import (
	"context"
	"strconv"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/router"
	"google.golang.org/grpc/metadata"
)

func TestConfirmedRootCutMetadataRoundTrip(t *testing.T) {
	ctx := router.WithConfirmedRootIndex(context.Background(), 42)
	outgoing := appendConfirmedRootCut(ctx)
	values, ok := metadata.FromOutgoingContext(outgoing)
	if !ok || len(values.Get(confirmedRootCutMetadataKey)) != 1 || values.Get(confirmedRootCutMetadataKey)[0] != "42" {
		t.Fatal("confirmed Root cut was not attached to internal RPC metadata", values)
	}
	incoming := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		confirmedRootCutMetadataKey, strconv.FormatUint(42, 10)))
	cut, found, err := confirmedRootCutFromMetadata(incoming)
	if err != nil || !found || cut != 42 {
		t.Fatal("confirmed Root cut did not survive RPC metadata", cut, found, err)
	}
}

func TestConfirmedRootCutMetadataRejectsMalformedCuts(t *testing.T) {
	for _, values := range [][]string{{"0"}, {"bad"}, {"1", "2"}} {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{confirmedRootCutMetadataKey: values})
		if _, _, err := confirmedRootCutFromMetadata(ctx); err == nil {
			t.Fatalf("malformed Root cuts accepted: %v", values)
		}
	}
}
