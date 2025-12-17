package pgqueue

import (
	"sync"
	"time"
)

type PriorityMetrics struct {
	Started   int64
	Succeeded int64
	Failed    int64
	Duration  time.Duration
}

type Metrics struct {
	mu   sync.RWMutex
	data map[Priority]map[TaskType]*PriorityMetrics
}

func NewMetrics() *Metrics {
	return &Metrics{
		data: make(map[Priority]map[TaskType]*PriorityMetrics),
	}
}

func (m *Metrics) get(p Priority, t TaskType) *PriorityMetrics {
	if _, ok := m.data[p]; !ok {
		m.data[p] = make(map[TaskType]*PriorityMetrics)
	}
	if _, ok := m.data[p][t]; !ok {
		m.data[p][t] = &PriorityMetrics{}
	}
	return m.data[p][t]
}

func (m *Metrics) RecordStart(p Priority, t TaskType) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.get(p, t).Started++
}

func (m *Metrics) RecordSuccess(p Priority, t TaskType, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pm := m.get(p, t)
	pm.Succeeded++
	pm.Duration += d
}

func (m *Metrics) RecordFailure(p Priority, t TaskType, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pm := m.get(p, t)
	pm.Failed++
	pm.Duration += d
}
