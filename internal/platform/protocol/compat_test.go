package protocol

import "testing"

func TestNegotiateRequiresMatchingDurableFormats(t *testing.T) {
	local := CurrentAdvertisement()
	if agreement, err := Negotiate(local, local); err != nil || agreement.Version != CurrentVersion {
		t.Fatalf("current protocol did not negotiate: agreement=%+v err=%v", agreement, err)
	}
	remote := local
	remote.PackFormat++
	if _, err := Negotiate(local, remote); err == nil {
		t.Fatal("mismatched Pack format was accepted")
	}
	remote = local
	remote.ProtocolMin = CurrentVersion + 1
	if _, err := Negotiate(local, remote); err == nil {
		t.Fatal("non-overlapping protocol range was accepted")
	}
	remote = local
	remote.RecordIndex++
	if _, err := Negotiate(local, remote); err == nil {
		t.Fatal("mismatched record index format was accepted")
	}
}
