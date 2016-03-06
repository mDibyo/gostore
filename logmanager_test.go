package gostore

import (
	"bytes"
	"github.com/golang/protobuf/proto"
	pb "github.com/mDibyo/gostore/pb"
	"reflect"
	"testing"
)

var (
	sampleKey1   = Key("key_1")
	sampleKey2   = Key("key_2")
	sampleValue1 = Value([]byte{0, 1, 2, 3, 4})
	sampleValue2 = Value([]byte{1, 2, 3, 4, 5})
	sampleValue3 = Value([]byte{2, 3, 4, 5, 6})
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
	smv := newStoreMapValue()
	smv.value = sampleValue1
	lm.storeMap[sampleKey1] = smv
	tid := NewTransaction()
	lm.beginTransaction(tid)
	wantLenLogAfter := len(lm.log.Entry)
	// Check value
	if gotV, err := lm.getValue(tid, sampleKey1); err != nil {
		t.Errorf("got an error while trying to get value: %v", err)
	} else if !bytes.Equal(gotV, sampleValue1) {
		t.Errorf("did not get back the correct value. expected=%v, actual=%v.", sampleValue1, gotV)
	}
	// Check log
	if gotLenLogAfter := len(lm.log.Entry); gotLenLogAfter != wantLenLogAfter {
		t.Errorf("did not get expected log length. expected=%d, actual=%d.", wantLenLogAfter, gotLenLogAfter)
	}
	// Check currMutexes
	if cm, ok := lm.currMutexes[tid]; !ok {
		t.Error("did not find transaction in current mutexes map as expected.")
	} else if rw, ok := cm[sampleKey1]; !ok {
		t.Error("did not find mutex for key in mutex map for transaction.")
	} else if !rw.rLocked() {
		t.Errorf("found that mutex for key was not read locked. mutex: %+v", rw)
	}
}

func TestSetValue(t *testing.T) {
	tests := []struct {
		key          Key
		value        Value
		wantLogEntry *pb.LogEntry
	}{
		{ // Add new key
			key:   sampleKey1,
			value: sampleValue1,
			wantLogEntry: &pb.LogEntry{
				EntryType: pb.LogEntry_UPDATE.Enum(),
				Key:       proto.String(string(sampleKey1)),
				NewValue:  sampleValue1,
			},
		},
		{ // Change value for existing key
			key:   sampleKey2,
			value: sampleValue2,
			wantLogEntry: &pb.LogEntry{
				EntryType: pb.LogEntry_UPDATE.Enum(),
				Key:       proto.String(string(sampleKey2)),
				OldValue:  sampleValue3,
				NewValue:  sampleValue2,
			},
		},
	}

	lm := newLogManager()
	smv := newStoreMapValue()
	smv.value = sampleValue3
	lm.storeMap[sampleKey2] = smv
	for _, test := range tests {
		tid := NewTransaction()
		lm.beginTransaction(tid)
		lenLogBefore := len(lm.log.Entry)
		// Check setting
		if err := lm.setValue(tid, test.key, test.value); err != nil {
			t.Errorf("got an error for (key='%s', value=%v) while trying to set value: %v.", test.key, test.value, err)
		}
		// Check storeMap
		if gotSMV, ok := lm.storeMap[test.key]; !ok {
			t.Errorf("did not find value for key='%s' in storeMap.", test.key)
		} else if !bytes.Equal(gotSMV.value, test.value) {
			t.Errorf("did not get back the correct value. key='%s', expected=%v, actual=%v.", test.key, test.value, gotSMV.value)
		}
		// Check log
		wantLogLenAfter := lenLogBefore + 1
		gotLenLogAfter := len(lm.log.Entry)
		if gotLenLogAfter != wantLogLenAfter {
			t.Fatalf("did not get expected log length. expected=%d, actual=%d.", wantLogLenAfter, gotLenLogAfter)
		}
		gotEntry := lm.log.Entry[gotLenLogAfter-1]
		wantEntry := test.wantLogEntry
		wantEntry.Lsn = gotEntry.Lsn
		wantEntry.Tid = proto.Int64(int64(tid))
		testLogEntry(t, gotEntry, wantEntry)
		// Check currMutexes
		if cm, ok := lm.currMutexes[tid]; !ok {
			t.Error("did not find transaction in current mutexes map as expected.")
		} else if rw, ok := cm[test.key]; !ok {
			t.Errorf("did not find mutex for key='%s' in mutex map for transaction.", test.key)
		} else if !rw.wLocked() {
			t.Errorf("found that mutex for key='%s' was not write locked. mutex: %+v", test.key, rw)
		}
	}
}
