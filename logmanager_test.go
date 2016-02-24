package gostore

import (
	"github.com/golang/protobuf/proto"
	pb "github.com/mDibyo/gostore/pb"
	"testing"
)

func TestAddLogEntry(t *testing.T) {
	tests := []struct {
		tid              int64
		entryType        pb.LogEntry_LogEntryType
		wantLenLogAfter  int
		wantNextLSNAfter int64
	}{
		{
			tid:       123,
			entryType: pb.LogEntry_BEGIN,
			wantLenLogAfter: 1,
			wantNextLSNAfter: 1,
		},
		{
			tid:       121,
			entryType: pb.LogEntry_END,
			wantLenLogAfter: 2,
			wantNextLSNAfter: 2,
		},
	}

	lm := logManager{
		currMutexes: make(map[TransactionID]currentMutexesMap),
		storeMap:    make(map[Key]*storeMapValue),
	}
	for _, test := range tests {
		lm.addLogEntry(&pb.LogEntry{
			Tid:       proto.Int64(test.tid),
			EntryType: test.entryType.Enum(),
		})
		if gotLenLogAfter := len(lm.log.Entry); gotLenLogAfter != test.wantLenLogAfter {
			t.Errorf("did not get expected log length. expected=%d, actual=%d", test.wantLenLogAfter, gotLenLogAfter)
		}
		if gotNextLSNAfter := int64(lm.nextLSN); gotNextLSNAfter != test.wantNextLSNAfter {
			t.Errorf("did not get expected next LSN. expected=%d, actual=%d", test.wantNextLSNAfter, gotNextLSNAfter)
		}
	}

}
