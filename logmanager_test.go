package gostore

import (
	"bytes"
	"github.com/golang/protobuf/proto"
	pb "github.com/mDibyo/gostore/pb"
	"reflect"
	"testing"
)

func testLogEntry(t *testing.T, gotEntry, wantEntry *pb.LogEntry) {
	if !reflect.DeepEqual(gotEntry, wantEntry) {
		t.Errorf("did not get the expected log entry. expected=(%+v), actual=(%+v)", wantEntry, gotEntry)
	}
}

func TestAddLogEntry(t *testing.T) {
	nextLSN := logSequenceNumber(5)
	tests := []struct {
		tid              int64
		entryType        pb.LogEntry_LogEntryType
		wantLenLogAfter  int
		wantNextLSNAfter logSequenceNumber
	}{
		{
			tid:              123,
			entryType:        pb.LogEntry_BEGIN,
			wantLenLogAfter:  1,
			wantNextLSNAfter: nextLSN + 1,
		},
		{
			tid:              121,
			entryType:        pb.LogEntry_END,
			wantLenLogAfter:  2,
			wantNextLSNAfter: nextLSN + 2,
		},
	}

	lm := *newLogManager()
	lm.nextLSN = nextLSN
	for _, test := range tests {
		lm.addLogEntry(&pb.LogEntry{
			Tid:       proto.Int64(test.tid),
			EntryType: test.entryType.Enum(),
		})
		if gotLenLogAfter := len(lm.log.Entry); gotLenLogAfter != test.wantLenLogAfter {
			t.Errorf("did not get expected log length. expected=%d, actual=%d", test.wantLenLogAfter, gotLenLogAfter)
		}
		if gotNextLSNAfter := lm.nextLSN; gotNextLSNAfter != test.wantNextLSNAfter {
			t.Errorf("did not get expected next LSN. expected=%d, actual=%d", test.wantNextLSNAfter, gotNextLSNAfter)
		}
	}
}

func TestBeginTransaction(t *testing.T) {
	lm := *newLogManager()
	tid := NewTransaction()
	lm.beginTransaction(tid)
	wantLogEntry := &pb.LogEntry{
		Lsn:       proto.Int64(0),
		Tid:       proto.Int64(int64(tid)),
		EntryType: pb.LogEntry_BEGIN.Enum(),
	}

	// Check log
	if gotLenLogAfter := len(lm.log.Entry); gotLenLogAfter != 1 {
		t.Errorf("did not get expected log length. expected=%d, actual=%d", 1, gotLenLogAfter)
	}
	testLogEntry(t, lm.log.Entry[0], wantLogEntry)
	// Check currMutexes
	if _, ok := lm.currMutexes[tid]; !ok {
		t.Errorf("did not find TransactionID %d in current mutexes map as expected", tid)
	}
}

func TestGetValue(t *testing.T) {
	lm := *newLogManager()
	k, v := Key("key_1"), Value([]byte{0, 1, 2, 3, 4})
	smv := newStoreMapValue()
	smv.value = v
	lm.storeMap[k] = smv
	tid := NewTransaction()
	lm.beginTransaction(tid)

	// Check value
	if gotV, err := lm.getValue(tid, k); err != nil {
		t.Errorf("got an error while trying to get value: %v", err)
	} else if !bytes.Equal(gotV, v) {
		t.Errorf("did not get back the correct value. expected=%v, actual=%v", v, gotV)
	}
	// Check log
	if gotLenLogAfter := len(lm.log.Entry); gotLenLogAfter != 1 {
		t.Errorf("did not get expected log length. expected=%d, actual=%d", 1, gotLenLogAfter)
	}
	// Check currMutexes
	if cm, ok := lm.currMutexes[tid]; !ok {
		t.Error("did not find transaction in current mutexes map as expected.")
	} else if rw, ok := cm[k]; !ok {
		t.Error("did not find mutex for key in mutex map for transaction.")
	} else if !rw.rLocked() {
		t.Logf("cm: %+v", cm)
		t.Errorf("found that mutex for key was not read locked. mutex: %+v", rw)
	}
}
