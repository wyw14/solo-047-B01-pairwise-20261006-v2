package store

import (
	"path/filepath"
	"testing"
	"time"
)

func TestAppendIfAbsentDeduplicatesAcrossRestarts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	at := time.Now().UTC()
	journal := NewJournal(path)

	appended, err := journal.AppendIfAbsent("telemetry", "sample-1", map[string]int{"v": 1}, at)
	if err != nil || !appended {
		t.Fatalf("first append: appended=%v err=%v", appended, err)
	}
	appended, err = journal.AppendIfAbsent("telemetry", "sample-1", map[string]int{"v": 2}, at)
	if err != nil || appended {
		t.Fatalf("duplicate append: appended=%v err=%v", appended, err)
	}
	exists, err := journal.HasEntity("telemetry", "sample-1")
	if err != nil || !exists {
		t.Fatalf("HasEntity: exists=%v err=%v", exists, err)
	}
	record, found, err := journal.Record("telemetry", "sample-1")
	if err != nil || !found {
		t.Fatalf("Record: found=%v err=%v", found, err)
	}
	if record.EntityID != "sample-1" {
		t.Fatalf("unexpected record: %+v", record)
	}
	if records, err := journal.Records(); err != nil || len(records) != 1 {
		t.Fatalf("journal rows=%d err=%v, want 1", len(records), err)
	}

	// A fresh instance over the same file must still reject duplicates.
	reopened := NewJournal(path)
	appended, err = reopened.AppendIfAbsent("telemetry", "sample-1", map[string]int{"v": 3}, at)
	if err != nil || appended {
		t.Fatalf("duplicate after restart: appended=%v err=%v", appended, err)
	}
	if exists, err := reopened.HasEntity("telemetry", "sample-1"); err != nil || !exists {
		t.Fatalf("HasEntity after restart: exists=%v err=%v", exists, err)
	}
}
