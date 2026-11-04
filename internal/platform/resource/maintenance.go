package resource

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

var ErrMaintenanceBusy = errors.New("background maintenance is busy or yielding to foreground traffic")

type MaintenanceStatus struct {
	Active         string `json:"active,omitempty"`
	Foreground     uint64 `json:"foreground"`
	BytesPerSecond uint64 `json:"bytes_per_second"`
	AccountedBytes uint64 `json:"accounted_bytes"`
	Deferred       uint64 `json:"deferred"`
}

// Maintenance serializes cooperating maintenance jobs and shapes their IO.
// It never delays foreground/Raft admission and is not a kernel IO/RSS limit.
type Maintenance struct {
	mu        sync.Mutex
	status    MaintenanceStatus
	tokens    float64
	updated   time.Time
	lastStart time.Time
	ioGate    chan struct{}
}

func NewMaintenance(rate uint64) *Maintenance {
	if rate == 0 {
		rate = 8 << 20
	}
	return &Maintenance{status: MaintenanceStatus{BytesPerSecond: rate}, tokens: float64(rate), updated: time.Now(), lastStart: time.Now(), ioGate: make(chan struct{}, 1)}
}

type maintenanceKey struct{}

func (m *Maintenance) TryStart(ctx context.Context, kind string) (context.Context, func(), error) {
	if ctx == nil || kind == "" {
		return nil, nil, errors.New("maintenance requires context and kind")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if m == nil {
		return ctx, func() {}, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.status.Active != "" || (m.status.Foreground != 0 && time.Since(m.lastStart) < 30*time.Second) {
		m.status.Deferred++
		return nil, nil, ErrMaintenanceBusy
	}
	m.status.Active = kind
	m.lastStart = time.Now()
	var once sync.Once
	return context.WithValue(ctx, maintenanceKey{}, m), func() { once.Do(func() { m.mu.Lock(); m.status.Active = ""; m.mu.Unlock() }) }, nil
}
func (m *Maintenance) Foreground() func() {
	if m == nil {
		return func() {}
	}
	m.mu.Lock()
	m.status.Foreground++
	m.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { m.mu.Lock(); m.status.Foreground--; m.mu.Unlock() }) }
}
func (m *Maintenance) Status() MaintenanceStatus { m.mu.Lock(); defer m.mu.Unlock(); return m.status }

// WaitIO must be called before taking Store/Pack locks. Calls without a
// maintenance context are unaffected. All cooperating jobs share one bucket.
func WaitIO(ctx context.Context, bytes uint64) error {
	if ctx == nil {
		return errors.New("nil IO context")
	}
	m, _ := ctx.Value(maintenanceKey{}).(*Maintenance)
	if m == nil {
		return ctx.Err()
	}
	// One FIFO wait at a time prevents a stream of small backup reads from
	// continually consuming credits needed by a larger repair operation.
	select {
	case m.ioGate <- struct{}{}:
		defer func() { <-m.ioGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	for bytes > 0 {
		m.mu.Lock()
		now := time.Now()
		rate := m.status.BytesPerSecond
		if m.status.Foreground != 0 {
			rate = max(uint64(1), rate/4)
		}
		m.tokens = min(float64(rate), m.tokens+now.Sub(m.updated).Seconds()*float64(rate))
		m.updated = now
		quantum := min(bytes, rate)
		if m.tokens >= float64(quantum) {
			m.tokens -= float64(quantum)
			m.status.AccountedBytes += quantum
			m.mu.Unlock()
			bytes -= quantum
			continue
		}
		wait := 20 * time.Millisecond
		if m.tokens < float64(quantum) {
			wait = max(wait, time.Duration((float64(quantum)-m.tokens)/float64(rate)*float64(time.Second)))
		}
		m.mu.Unlock()
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}

type maintenanceReader struct {
	ctx    context.Context
	reader io.ReadCloser
}

func LimitReader(ctx context.Context, reader io.ReadCloser) io.ReadCloser {
	return &maintenanceReader{ctx: ctx, reader: reader}
}
func (r *maintenanceReader) Read(p []byte) (int, error) {
	p = p[:min(len(p), 256<<10)]
	if err := WaitIO(r.ctx, uint64(len(p))); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
func (r *maintenanceReader) Close() error { return r.reader.Close() }

// WithMaintenance shares IO shaping for authenticated remote repair without taking a second job slot across a network call.
func WithMaintenance(ctx context.Context, m *Maintenance) context.Context {
	if m == nil {
		return ctx
	}
	return context.WithValue(ctx, maintenanceKey{}, m)
}

func EnterForeground(ctx context.Context, m *Maintenance) func() {
	if ctx != nil {
		if scheduler, _ := ctx.Value(maintenanceKey{}).(*Maintenance); scheduler != nil {
			return func() {}
		}
	}
	return m.Foreground()
}

func IsMaintenance(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	m, _ := ctx.Value(maintenanceKey{}).(*Maintenance)
	return m != nil
}
