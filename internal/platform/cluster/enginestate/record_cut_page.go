package enginestate

import (
	"bytes"
	"context"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
)

const MaxRecordCutPage = 1024

// RecordCutCursor names a position in ONE immutable, verified artifact. It is
// an untrusted traversal hint, never evidence that earlier records were copied
// or still meet a physical majority. A future durable coverage protocol must
// separately bind verified holdings and their retention/invalidations.
type RecordCutCursor struct {
	Version        int    `json:"version"`
	SealRequestID  string `json:"seal_request_id"`
	ArtifactSHA256 string `json:"artifact_sha256"`
	SetSHA256      string `json:"set_sha256"`
	LastKey        []byte `json:"last_key"`
}

type RecordCutPage struct {
	Cursor       *RecordCutCursor
	Count        uint64
	PayloadBytes uint64
	Done         bool
}

func (c RecordCutCursor) matches(cut RecordCut) bool {
	return c.Version == 1 && c.SealRequestID == cut.Seal.SealRequestID &&
		c.ArtifactSHA256 == cut.Artifact.SHA256 && c.SetSHA256 == cut.SHA256 &&
		bytes.HasPrefix(c.LastKey, recordReferencePrefix) && len(c.LastKey) <= maxLiveReferenceKeyBytes()
}

func cursorForCut(cut RecordCut, key []byte) *RecordCutCursor {
	return &RecordCutCursor{Version: 1, SealRequestID: cut.Seal.SealRequestID,
		ArtifactSHA256: cut.Artifact.SHA256, SetSHA256: cut.SHA256, LastKey: bytes.Clone(key)}
}

// WalkVerifiedRecordCutPage bounds the number of caller callbacks for an
// immutable sealed manifest. It validates the entire artifact before callbacks
// and again while traversing; it does NOT avoid a complete artifact scan per
// page. An error returns no completed page, even if callbacks already ran.
// Reusing a cursor never authorizes skipping physical re-verification at
// generation publication.
func WalkVerifiedRecordCutPage(ctx context.Context, artifacts *snapshot.Store, cut RecordCut,
	cursor *RecordCutCursor, limit uint32, visit func(RecordReference) error) (RecordCutPage, error) {
	cut.Seal = cloneCutSeal(cut.Seal)
	if cursor != nil {
		owned := *cursor
		owned.LastKey = bytes.Clone(cursor.LastKey)
		cursor = &owned
	}
	if ctx == nil || artifacts == nil || cut.Validate() != nil || limit == 0 || limit > MaxRecordCutPage || visit == nil ||
		(cursor != nil && !cursor.matches(cut)) {
		return RecordCutPage{}, errors.New("invalid sealed record page request or cursor")
	}
	if err := WalkRecordCut(ctx, artifacts, cut, nil); err != nil {
		return RecordCutPage{}, err
	}
	var page RecordCutPage
	if cursor != nil {
		page.Cursor = cursorForCut(cut, cursor.LastKey)
	}
	foundCursor := cursor == nil
	page.Done = true
	err := WalkRecordCut(ctx, artifacts, cut, func(ref RecordReference) error {
		key := recordReferenceKey(ref.Descriptor.Keyspace, ref.Descriptor.Key)
		if !foundCursor {
			switch bytes.Compare(key, cursor.LastKey) {
			case -1:
				return nil
			case 0:
				foundCursor = true
				return nil
			default:
				return errors.New("sealed record cursor is not present in this artifact")
			}
		}
		if page.Count == uint64(limit) {
			page.Done = false
			return nil
		}
		if err := visit(ref); err != nil {
			return err
		}
		if page.PayloadBytes > ^uint64(0)-ref.Descriptor.Size {
			return errors.New("sealed record page byte count overflow")
		}
		page.Count++
		page.PayloadBytes += ref.Descriptor.Size
		page.Cursor = cursorForCut(cut, key)
		return nil
	})
	if err != nil {
		return RecordCutPage{}, err
	}
	if !foundCursor {
		return RecordCutPage{}, errors.New("sealed record cursor is not present in this artifact")
	}
	return page, nil
}
