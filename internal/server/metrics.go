package server

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

// monitoringView is deliberately read-only. Monitoring must never become a
// second control plane or acquire a Store/Volume lock on behalf of a scrape.
type monitoringView struct {
	runtime        *node.Runtime
	ready          <-chan struct{}
	repairSnapshot func() repairObservation
}

func (view monitoringView) readyState() bool {
	if view.ready == nil {
		return false
	}
	select {
	case <-view.ready:
		return true
	default:
		return false
	}
}

func writeMonitoringEndpoint(w http.ResponseWriter, r *http.Request, view monitoringView) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	switch r.URL.Path {
	case "/healthz":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	case "/readyz":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if !view.readyState() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready\n"))
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	case "/metrics":
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		writePrometheusMetrics(w, view)
	default:
		http.NotFound(w, r)
	}
}

func writePrometheusMetrics(w http.ResponseWriter, view monitoringView) {
	writeGauge := func(name string, value float64, labels string) {
		if labels != "" {
			_, _ = fmt.Fprintf(w, "%s{%s} %s\n", name, labels, strconv.FormatFloat(value, 'f', -1, 64))
			return
		}
		_, _ = fmt.Fprintf(w, "%s %s\n", name, strconv.FormatFloat(value, 'f', -1, 64))
	}
	comment := func(name, help string) { _, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", name, help, name) }

	comment("dubi_node_up", "Whether the DUBI process is serving monitoring requests.")
	writeGauge("dubi_node_up", 1, "")
	comment("dubi_node_ready", "Whether the Node completed startup and local Group recovery.")
	if view.readyState() {
		writeGauge("dubi_node_ready", 1, "")
	} else {
		writeGauge("dubi_node_ready", 0, "")
	}

	if view.runtime == nil {
		comment("dubi_root_observed", "Whether the local runtime exposed Root state to the monitor.")
		writeGauge("dubi_root_observed", 0, "")
		return
	}
	comment("dubi_root_observed", "Whether the local runtime exposed Root state to the monitor.")
	writeGauge("dubi_root_observed", 1, "")
	comment("dubi_root_applied_index", "Applied Root state index on this Node.")
	writeGauge("dubi_root_applied_index", float64(view.runtime.Root.AppliedIndex()), "")
	if status, err := view.runtime.Host.Status(raft.RootGroupID); err == nil {
		comment("dubi_root_commit_index", "Committed Root Raft index on this Node.")
		writeGauge("dubi_root_commit_index", float64(status.CommitIndex), "")
		comment("dubi_root_term", "Current Root Raft term on this Node.")
		writeGauge("dubi_root_term", float64(status.Term), "")
		comment("dubi_root_leader_id", "Root Raft leader ID observed by this Node.")
		writeGauge("dubi_root_leader_id", float64(status.LeaderID), "")
		comment("dubi_root_role", "Root Raft role, labelled by role name.")
		writeGauge("dubi_root_role", 1, `role="`+escapeLabel(status.Role.String())+`"`)
	}
	if membership, err := view.runtime.Host.Membership(raft.RootGroupID); err == nil {
		comment("dubi_root_voters", "Number of Root voters in the local membership view.")
		writeGauge("dubi_root_voters", float64(len(membership.GetVoters())), "")
		comment("dubi_root_learners", "Number of Root learners in the local membership view.")
		writeGauge("dubi_root_learners", float64(len(membership.GetLearners())), "")
	}

	if view.runtime.Volumes != nil {
		if report, err := view.runtime.Volumes.Status(); err == nil {
			comment("dubi_volume_health", "Volume health by Volume ID, state and health.")
			comment("dubi_volume_capacity_bytes", "Total filesystem capacity for a Volume.")
			comment("dubi_volume_available_bytes", "Available filesystem capacity for a Volume.")
			for _, item := range report.Volumes {
				labels := `volume_id="` + escapeLabel(item.ID) + `",state="` + escapeLabel(string(item.State)) + `",health="` + escapeLabel(item.Health) + `"`
				writeGauge("dubi_volume_health", 1, labels)
				writeGauge("dubi_volume_capacity_bytes", float64(item.Capacity.TotalBytes), `volume_id="`+escapeLabel(item.ID)+`"`)
				writeGauge("dubi_volume_available_bytes", float64(item.Capacity.AvailableBytes), `volume_id="`+escapeLabel(item.ID)+`"`)
			}
		}
	}
	if view.runtime.Host != nil {
		disk := view.runtime.Host.DiskStatus()
		comment("dubi_disk_pressured", "Whether Store disk admission is under pressure.")
		if disk.Pressured {
			writeGauge("dubi_disk_pressured", 1, "")
		} else {
			writeGauge("dubi_disk_pressured", 0, "")
		}
		comment("dubi_disk_available_bytes", "Available bytes reported by Store disk admission.")
		writeGauge("dubi_disk_available_bytes", float64(disk.AvailableBytes), "")
		comment("dubi_disk_reserve_bytes", "Reserved bytes required before discretionary writes are rejected.")
		writeGauge("dubi_disk_reserve_bytes", float64(disk.ReserveBytes), "")
	}
	if view.repairSnapshot != nil {
		repair := view.repairSnapshot()
		comment("dubi_node_repair_debt", "Number of unresolved local immutable-record repair issues.")
		writeGauge("dubi_node_repair_debt", float64(len(repair.Issues)), "")
		comment("dubi_node_repair_last_check_unix", "Unix time of the last local repair observation.")
		writeGauge("dubi_node_repair_last_check_unix", float64(repair.LastCheck.Unix()), "")
	}
}

func escapeLabel(value string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(value)
}
