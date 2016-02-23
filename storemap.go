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

type storeMapValue struct {
	value Value

	// RWMutex attributes
	lock sync.RWMutex

	// ValueAccessor attributes
	rAccessorChan chan *valueAccessor
	wAccessorChan chan *valueAccessor
	ping          chan struct{}
}

// rwMutexWrapper is a thread-safe convenience wrapper for sync.RWMutex used in StoreMapValue.
type rwMutexWrapper struct {
	selfLock sync.Mutex     // Self Lock to synchronize lock and unlock operations.
	smv      *storeMapValue // storeMapValue to which the lock belongs.
	held     bool           // Whether the lock is held.
	wAllowed bool           // Whether writes are allowed.
	orgValue Value          // Original Value stored in smv. Only defined when writes are allowed.
}

func wrapRWMutex(smv *storeMapValue) rwMutexWrapper {
	return rwMutexWrapper{smv: smv}
}

func (rw *rwMutexWrapper) rLock() {
	rw.selfLock.Lock()
	defer rw.selfLock.Unlock()

	if rw.held {
		return
	}
	rw.rLockUnsafe()
}

func (rw *rwMutexWrapper) rLockUnsafe() {
	rw.smv.lock.RLock()
	rw.held = true
}

func (rw *rwMutexWrapper) rUnlock() {
	rw.selfLock.Lock()
	defer rw.selfLock.Unlock()

	if !rw.held {
		return
	}
	rw.rUnlockUnsafe()
}

func (rw *rwMutexWrapper) rUnlockUnsafe() {
	rw.smv.lock.RUnlock()
	rw.held = false
}

func (rw *rwMutexWrapper) wLock() {
	rw.selfLock.Lock()
	defer rw.selfLock.Unlock()

	if rw.held && rw.wAllowed {
		return
	}
	rw.wLockUnsafe()
}

func (rw *rwMutexWrapper) wLockUnsafe() {
	rw.smv.lock.Lock()
	rw.held = true
	rw.wAllowed = true
	rw.orgValue = rw.smv.value
}

func (rw *rwMutexWrapper) wUnlock() {
	rw.selfLock.Lock()
	defer rw.selfLock.Unlock()

	if !rw.held || !rw.wAllowed {
		return
	}
	rw.wUnlockUnsafe()
}

func (rw *rwMutexWrapper) wUnlockUnsafe() {
	rw.smv.lock.Unlock()
	rw.held = false
	rw.wAllowed = false
	rw.orgValue = nil
}

func (rw *rwMutexWrapper) promote() {
	rw.selfLock.Lock()
	defer rw.selfLock.Unlock()

	if rw.wAllowed {
		return
	}
	rw.rUnlockUnsafe()
	rw.wLockUnsafe()
}

type logSequenceNumber int64

var currentLSN logSequenceNumber = 0

type logManager struct {
	log            pb.Log
	currTAccessors map[TransactionID]map[Key]rwMutexWrapper
	storeMap       map[Key]storeMapValue
}

func (lm logManager) beginTransaction(tid TransactionID) {
	entries := lm.log.GetEntry()
	entries = append(entries, &pb.LogEntry{
		Lsn:       proto.Int64(int64(currentLSN)),
		Tid:       proto.Int64(int64(tid)),
		EntryType: pb.LogEntry_BEGIN.Enum(),
	})
	lm.currTAccessors[tid] = make(map[Key]rwMutexWrapper)

	currentLSN++
}

var lm logManager

func init() {
	lm = logManager{
		currTAccessors: make(map[TransactionID]map[Key]rwMutexWrapper),
		storeMap:       make(map[Key]storeMapValue),
	}
}
