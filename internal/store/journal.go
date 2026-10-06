package store

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Record struct {
	Kind      string          `json:"kind"`
	EntityID  string          `json:"entity_id"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

type Journal struct {
	mu     sync.Mutex
	path   string
	seen   map[string]struct{}
	loaded bool
}

func NewJournal(path string) *Journal {
	return &Journal{path: path}
}

func journalKey(kind, entityID string) string {
	return kind + "\x00" + entityID
}

// loadSeen rebuilds the set of persisted (kind, entityID) keys from disk so
// that idempotent appends stay deduplicated across restarts. Callers must hold
// j.mu.
func (j *Journal) loadSeen() error {
	if j.loaded {
		return nil
	}
	records, err := j.readRecordsLocked()
	if err != nil {
		return err
	}
	j.seen = make(map[string]struct{}, len(records))
	for _, record := range records {
		j.seen[journalKey(record.Kind, record.EntityID)] = struct{}{}
	}
	j.loaded = true
	return nil
}

// HasEntity reports whether a record with the given kind and entity ID has
// already been persisted.
func (j *Journal) HasEntity(kind, entityID string) (bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.loadSeen(); err != nil {
		return false, err
	}
	_, exists := j.seen[journalKey(kind, entityID)]
	return exists, nil
}

// Record returns a previously persisted record selected by kind and entity
// ID. When several records share the key, the first (oldest) is returned.
func (j *Journal) Record(kind, entityID string) (Record, bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	records, err := j.readRecordsLocked()
	if err != nil {
		return Record{}, false, err
	}
	for _, record := range records {
		if record.Kind == kind && record.EntityID == entityID {
			return record, true, nil
		}
	}
	return Record{}, false, nil
}

// AppendIfAbsent persists a record only when no record with the same kind and
// entity ID exists. It reports whether a new record was appended, so callers
// can distinguish a fresh write from an idempotent replay. The existence check
// and the append happen under the same lock.
func (j *Journal) AppendIfAbsent(kind, entityID string, value any, at time.Time) (bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.loadSeen(); err != nil {
		return false, err
	}
	key := journalKey(kind, entityID)
	if _, exists := j.seen[key]; exists {
		return false, nil
	}
	if err := j.appendLocked(kind, entityID, value, at); err != nil {
		return false, err
	}
	j.seen[key] = struct{}{}
	return true, nil
}

func (j *Journal) Append(kind, entityID string, value any, at time.Time) error {
	if kind == "" || entityID == "" {
		return errors.New("journal kind and entity ID are required")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.loadSeen(); err != nil {
		return err
	}
	if err := j.appendLocked(kind, entityID, value, at); err != nil {
		return err
	}
	j.seen[journalKey(kind, entityID)] = struct{}{}
	return nil
}

func (j *Journal) appendLocked(kind, entityID string, value any, at time.Time) error {
	if kind == "" || entityID == "" {
		return errors.New("journal kind and entity ID are required")
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode journal payload: %w", err)
	}
	record := Record{Kind: kind, EntityID: entityID, Timestamp: at.UTC(), Payload: payload}
	line, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode journal record: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(j.path), 0o755); err != nil {
		return fmt.Errorf("create journal directory: %w", err)
	}
	file, err := os.OpenFile(j.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open journal: %w", err)
	}
	defer file.Close()
	if _, err := file.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("append journal: %w", err)
	}
	return file.Sync()
}

func (j *Journal) Records() ([]Record, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.readRecordsLocked()
}

func (j *Journal) readRecordsLocked() ([]Record, error) {
	file, err := os.Open(j.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", err)
	}
	defer file.Close()
	var records []Record
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record Record
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return nil, fmt.Errorf("decode journal record: %w", err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read journal: %w", err)
	}
	return records, nil
}
