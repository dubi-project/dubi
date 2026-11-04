package volume

import "testing"

func TestBalanceUsesDistinctHealthyDevicesAndHysteresis(t *testing.T) {
	makeVolume := func(id string, device, total, free uint64) Status {
		return Status{Record: Record{ID: id, State: Active}, Health: "healthy", Device: device, Capacity: Capacity{TotalBytes: total, AvailableBytes: free}}
	}
	report := Report{Volumes: []Status{makeVolume("source", 1, 100<<30, 15<<30), makeVolume("target", 2, 100<<30, 85<<30)}}
	plan, ok := PlanBalance(report)
	if !ok || plan.Source != "source" || plan.Target != "target" || plan.MaximumBytes > 18<<30 {
		t.Fatal(plan, ok)
	}
	report.Volumes[1].Device = 1
	if _, ok := PlanBalance(report); ok {
		t.Fatal("two directories on one filesystem treated as spare disk")
	}
	report.Volumes[1].Device = 2
	report.Volumes[1].Capacity.AvailableBytes = 20 << 30
	if _, ok := PlanBalance(report); ok {
		t.Fatal("small usage differences caused rebalance oscillation")
	}
	report.Volumes[1].Capacity.AvailableBytes = 85 << 30
	report.Volumes[1].State = Draining
	if _, ok := PlanBalance(report); ok {
		t.Fatal("draining target selected")
	}
	report.Volumes[1].State = Active
	report.Volumes[1].Health = "missing"
	if _, ok := PlanBalance(report); ok {
		t.Fatal("unreadable target selected")
	}
}

func TestWritableCapacityUsesLargestAdmissibleDisk(t *testing.T) {
	report := Report{Volumes: []Status{{Record: Record{ID: "a", State: Active}, Health: "healthy", Capacity: Capacity{TotalBytes: 10 << 30, AvailableBytes: 5 << 30}},
		{Record: Record{ID: "b", State: Active}, Health: "healthy", Capacity: Capacity{TotalBytes: 10 << 30, AvailableBytes: 3 << 30}}}}
	if got := WritableCapacity(report, nil); got != 4<<30 {
		t.Fatal("capacity hint summed independent disks or omitted reserve", got)
	}
	if got := WritableCapacity(report, map[string]string{"a": "quarantined"}); got != 2<<30 {
		t.Fatal(got)
	}
	report.Volumes[1].Capacity.AvailableBytes = MinFreeBytes - 1
	if got := WritableCapacity(report, map[string]string{"a": "quarantined"}); got != 0 {
		t.Fatal("disk safety floor bypassed", got)
	}
}
