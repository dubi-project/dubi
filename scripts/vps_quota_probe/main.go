// vps_quota_probe runs HTTPS checks from a VPS using the cluster's verified
// Node TLS identity. It never disables certificate verification or logs tokens.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/identity"
)

type Credential struct {
	Token string `json:"token"`
}
type Case struct {
	Label, Credential, Key string
	Count, Size            int
}
type Plan struct {
	Node, Phase string
	Cases       []Case
}
type Sample struct {
	Node         string  `json:"node"`
	Phase        string  `json:"phase"`
	Case         string  `json:"case"`
	Operation    string  `json:"operation"`
	Index        int     `json:"index"`
	Bytes        int     `json:"bytes"`
	Milliseconds float64 `json:"ms"`
	Status       int     `json:"status"`
	Error        string  `json:"error,omitempty"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	store := flag.String("store", "/var/lib/dubi", "node metadata")
	credentials := flag.String("credentials", "", "private JSON credential map")
	planFile := flag.String("plan", "", "benchmark plan JSON")
	callFile := flag.String("call", "", "single HTTP request JSON")
	address := flag.String("address", "https://127.0.0.1:12444", "local public API")
	flag.Parse()
	id, err := identity.Load(*store)
	if err != nil {
		return err
	}
	_, tls, err := config.LoadTLS(*store, id)
	if err != nil {
		return err
	}
	tls.Certificates = nil
	tls.GetClientCertificate = nil
	transport := &http.Transport{TLSClientConfig: tls, MaxIdleConnsPerHost: 4}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 18 * time.Second}
	b, err := os.ReadFile(*credentials)
	if err != nil {
		return err
	}
	creds := map[string]Credential{}
	if err = json.Unmarshal(b, &creds); err != nil {
		return err
	}
	request := func(method, path, credential string, body any) (int, []byte, error) {
		c, ok := creds[credential]
		if !ok || c.Token == "" {
			return 0, nil, fmt.Errorf("missing credential alias %s", credential)
		}
		var encoded []byte
		if body != nil {
			encoded, err = json.Marshal(body)
			if err != nil {
				return 0, nil, err
			}
		}
		req, err := http.NewRequest(method, *address+path, bytes.NewReader(encoded))
		if err != nil {
			return 0, nil, err
		}
		req.Header.Set("Authorization", "Bearer "+c.Token)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		res, err := client.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer res.Body.Close()
		b, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		return res.StatusCode, b, err
	}
	if *callFile != "" {
		var call struct {
			Method, Path, Credential string
			Body                     any
		}
		b, err := os.ReadFile(*callFile)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(b, &call); err != nil {
			return err
		}
		start := time.Now()
		status, b, err := request(call.Method, call.Path, call.Credential, call.Body)
		if err != nil {
			return err
		}
		// Calls may issue a token: the controller must save this output privately.
		var decoded any
		if json.Unmarshal(b, &decoded) != nil {
			decoded = string(b)
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"status": status, "ms": float64(time.Since(start).Microseconds()) / 1000, "body": decoded})
	}
	var plan Plan
	b, err = os.ReadFile(*planFile)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(b, &plan); err != nil {
		return err
	}
	failures := 0
	for _, c := range plan.Cases {
		if c.Count < 1 || c.Count > 1000 || c.Size < 1 || c.Size > 65536 {
			return fmt.Errorf("invalid benchmark bounds")
		}
		for i := -2; i < c.Count; i++ {
			// Two warmups per case are recorded but excluded from percentiles.
			key := fmt.Sprintf("quota-bench/%s/%s/%s/%d", plan.Phase, plan.Node, c.Key, i)
			value := bytes.Repeat([]byte("v"), c.Size)
			sum := sha256.Sum256([]byte(key))
			requestID := "bench-" + hex.EncodeToString(sum[:])
			path := "/api/v1/config/entry?key=" + url.QueryEscape(key)
			writeOK := false
			for _, method := range []string{"PUT", "GET"} {
				var body any
				if method == "PUT" {
					body = map[string]any{"request_id": requestID, "value": value}
				}
				start := time.Now()
				status, b, e := request(method, path, c.Credential, body)
				sample := Sample{Node: plan.Node, Phase: plan.Phase, Case: c.Label, Operation: method, Index: i, Bytes: c.Size, Milliseconds: float64(time.Since(start).Microseconds()) / 1000, Status: status}
				if e != nil {
					sample.Error = e.Error()
				} else if status != 200 {
					var problem struct {
						Code string `json:"code"`
					}
					_ = json.Unmarshal(b, &problem)
					sample.Error = problem.Code
					if sample.Error == "" {
						sample.Error = "unexpected_http_status"
					}
				} else if method == "PUT" {
					writeOK = true
				} else {
					var entry struct {
						Value []byte `json:"value"`
					}
					if json.Unmarshal(b, &entry) != nil || !bytes.Equal(entry.Value, value) {
						sample.Error = "read_value_mismatch"
					}
				}
				if method == "GET" && !writeOK && sample.Error == "" {
					sample.Error = "write_outcome_was_unknown"
				}
				if sample.Error != "" {
					failures++
				}
				if err = json.NewEncoder(os.Stdout).Encode(sample); err != nil {
					return err
				}
			}
		}
	}
	if failures > 0 {
		return fmt.Errorf("%d benchmark requests failed; see JSONL", failures)
	}
	return nil
}
