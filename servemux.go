package pgqueue

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
)

// ServeMux is a multiplexer for tasks which matches the type of each task against a list of registered patterns
// and calls the workerhandler for the pattern that most closely matches the task's type.
type ServeMux struct {
	mu          sync.RWMutex
	m           map[string]muxEntry
	es          []muxEntry
	middlewares []Middleware
}

type muxEntry struct {
	h       WorkerHandler
	pattern string
}

// NewServeMux allocates and returns a new ServeMux.
func NewServeMux() *ServeMux {
	mux := &ServeMux{
		m: make(map[string]muxEntry),
	}

	mux.Use(recoverMiddleware())
	return mux
}

// ProcessTask dispatches the task to the handler whose
// pattern most closely matches the task type.
func (mux *ServeMux) ProcessTask(ctx context.Context, task *Task) error {
	h, pattern := mux.handler(task)
	if pattern == "" {
		log.Printf("[WARN] handler not found for task %s", task.Type)
		return fmt.Errorf("handler not found for task %s", task.Type)
	}

	return h.ProcessTask(ctx, task)
}

// Handler returns the handler to use for the given task.
// It always return a non-nil handler.
//
// Handler also returns the registered pattern that matches the task.
//
// If there is no registered handler that applies to the task,
// handler returns a 'not found' handler which returns an error.
func (mux *ServeMux) handler(t *Task) (h WorkerHandler, pattern string) {
	mux.mu.RLock()
	defer mux.mu.RUnlock()

	h, pattern = mux.match(string(t.Type))
	if h == nil {
		h, pattern = NotFoundHandler(), ""
	}

	for i := len(mux.middlewares) - 1; i >= 0; i-- {
		h = mux.middlewares[i](h)
	}

	return h, pattern
}

// Find a handler on a handler map given a typename string.
// Most-specific (longest) pattern wins.
func (mux *ServeMux) match(typename string) (h WorkerHandler, pattern string) {
	v, ok := mux.m[typename]
	if ok {
		return v.h, v.pattern
	}

	for _, e := range mux.es {
		if strings.HasPrefix(typename, e.pattern) {
			return e.h, e.pattern
		}
	}
	return nil, ""
}

// Handle registers the handler for the given pattern.
// If a handler already exists for pattern, Handle panics.
func (mux *ServeMux) handle(pattern string, handler WorkerHandler) {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	if strings.TrimSpace(pattern) == "" {
		panic("pgqueue: invalid pattern")
	}
	if handler == nil {
		panic("pgqueue: nil handler")
	}
	if _, exist := mux.m[pattern]; exist {
		panic("pgqueue: multiple registrations for " + pattern)
	}

	if mux.m == nil {
		mux.m = make(map[string]muxEntry)
	}
	e := muxEntry{h: handler, pattern: pattern}
	mux.m[pattern] = e
	mux.es = appendSorted(mux.es, e)
}

func appendSorted(es []muxEntry, e muxEntry) []muxEntry {
	n := len(es)
	i := sort.Search(n, func(i int) bool {
		return len(es[i].pattern) < len(e.pattern)
	})
	if i == n {
		return append(es, e)
	}

	es = append(es, muxEntry{})
	copy(es[i+1:], es[i:])
	es[i] = e
	return es
}

// HandleFunc registers the handler function for the given pattern.
func (mux *ServeMux) HandleFunc(pattern string, handler func(context.Context, *Task) error) {
	if handler == nil {
		panic("pgqueue: nil handler")
	}
	mux.handle(pattern, HandlerFunc(handler))
}

// Use appends middleware to the mux.
// Middleware runs in the order it is added.
func (mux *ServeMux) Use(mw ...Middleware) {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	mux.middlewares = append(mux.middlewares, mw...)
}

// notFound returns an error indicating that the handler was not found for the given task.
func notFound(ctx context.Context, task *Task) error {
	return fmt.Errorf("%w %q", ErrHandlerNotFound, string(task.Type))
}

// NotFoundHandler returns a simple task handler that returns a “not found“ error.
func NotFoundHandler() WorkerHandler { return HandlerFunc(notFound) }
