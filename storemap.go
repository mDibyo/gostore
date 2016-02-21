/*
Package gostore implements a simple single-node log-based key-value
store. It supports multiple concurrent transactions through a set of
locks on values.
*/
package gostore

import (
	"github.com/golang/protobuf/proto"
	pb "github.com/mDibyo/gostore/pb"
	"sync"
)

// Key represents a key in the store
type Key string

// Value represents the value for a key in the key store
type Value []byte

type StoreMapValue struct {
	value Value
	lock  sync.RWMutex
}

type logSequenceNumber int64

var currentLSN logSequenceNumber = 0

type logManager struct {
	log         pb.Log
	currentTMap map[TransactionID]logSequenceNumber
	storeMap    map[Key]StoreMapValue
}

func (lm logManager) beginTransaction(tid TransactionID) {
	entries := lm.log.GetEntry()
	entries = append(entries, &pb.LogEntry{
		Lsn:       proto.Int64(int64(currentLSN)),
		Tid:       proto.Int64(int64(tid)),
		EntryType: pb.LogEntry_BEGIN.Enum(),
	})
	lm.currentTMap[tid] = currentLSN

	currentLSN++
}

var lm logManager

func init() {
	lm = logManager{
		pb.Log{},
		make(map[TransactionID]logSequenceNumber),
		make(map[Key]StoreMapValue),
	}
}
