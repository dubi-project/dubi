package command

import "testing"

func TestSummarizeDoctorDeduplicatesObservedReasons(t *testing.T) {
	report := statusReport{
		Cluster:           clusterHealthView{State: "degraded", Reasons: []string{"Node n2 repair status is unreachable", "volume missing"}},
		Health:            localHealthView{State: "degraded", Reasons: []string{"volume missing", "repair status unavailable"}},
		LocalStorageError: "volume missing",
	}
	got := summarizeDoctor(report)
	if got.State != "degraded" || len(got.Reasons) != 3 {
		t.Fatalf("unexpected doctor result: %+v", got)
	}
	if got.Reasons[0] != "Node n2 repair status is unreachable" || got.Reasons[1] != "volume missing" || got.Reasons[2] != "repair status unavailable" {
		t.Fatalf("reasons were not stable and deduplicated: %+v", got.Reasons)
	}
}

func TestSummarizeDoctorTreatsIncompleteObservationAsUnknown(t *testing.T) {
	got := summarizeDoctor(statusReport{Cluster: clusterHealthView{State: "available"}, Health: localHealthView{State: "unknown"}})
	if got.State != "unknown" || len(got.Reasons) != 1 || got.Reasons[0] != "cluster health is not fully observed" {
		t.Fatalf("incomplete observation was not surfaced: %+v", got)
	}
}

func TestDoctorExitErrorOnlyPassesHealthyReport(t *testing.T) {
	if err := doctorExitError(doctorResult{State: "healthy"}); err != nil {
		t.Fatal(err)
	}
	if err := doctorExitError(doctorResult{State: "degraded"}); err == nil {
		t.Fatal("degraded doctor result was treated as healthy")
	}
	if err := doctorExitError(doctorResult{State: "unknown"}); err == nil {
		t.Fatal("unknown doctor result was treated as healthy")
	}
}
