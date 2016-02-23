/*
Package gostore implements a simple single-node log-based key-value
store. It supports multiple concurrent transactions through a set of
locks on values.
*/
package gostore

import (
	"fmt"
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

func newStoreMapValue() storeMapValue {
	return storeMapValue{
		rAccessorChan: make(chan *valueAccessor),
		wAccessorChan: make(chan *valueAccessor),
		ping:          make(chan struct{}),
	}
}

// rwMutexWrapper is a thread-safe convenience wrapper for sync.RWMutex used in StoreMapValue.
type rwMutexWrapper struct {
	selfLock sync.Mutex     // Self Lock to synchronize lock and unlock operations.
	smv      *storeMapValue // storeMapValue to which the lock belongs.
	held     bool           // Whether the lock is held.
	wAllowed bool           // Whether writes are allowed.
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

type currentMutexesMap map[Key]rwMutexWrapper

type logManager struct {
	log         pb.Log                              // the log of transaction operations
	currMutexes map[TransactionID]currentMutexesMap // the mutexes held currently by running transactions
	storeMap    map[Key]storeMapValue
}

func (cm currentMutexesMap) GetMutex(k Key, smv *storeMapValue) *rwMutexWrapper {
	rw, ok := cm[k]
	if !ok {
		rw = wrapRWMutex(smv)
		cm[k] = rw
	}
	return &rw
}

var currentLSN logSequenceNumber = 0

func (lm logManager) addLogEntry(e *pb.LogEntry) {
	entries := lm.log.GetEntry()
	e.Lsn = proto.Int64(int64(currentLSN))
	entries = append(entries, e)
	currentLSN++
}

func (lm logManager) beginTransaction(tid TransactionID) {
	lm.addLogEntry(&pb.LogEntry{
		Tid:       proto.Int64(int64(tid)),
		EntryType: pb.LogEntry_BEGIN.Enum(),
	})
	lm.currMutexes[tid] = make(currentMutexesMap)
}

func (lm logManager) getValue(tid TransactionID, k Key) (v Value, err error) {
	cm, ok := lm.currMutexes[tid]
	if !ok {
		err = fmt.Errorf("transaction with ID %d is not currently running", tid)
		return
	}
	smv, ok := lm.storeMap[k]
	if !ok {
		err = fmt.Errorf("key %s does not exist", k)
		return
	}
	rw := *cm.GetMutex(k, &smv)
	rw.rLock()
	v = smv.value
	return
}

func (lm logManager) setValue(tid TransactionID, k Key, v Value) (err error) {
	cm, ok := lm.currMutexes[tid]
	if !ok {
		err = fmt.Errorf("transaction with ID %d is not currently running", tid)
	}
	if v == nil {
		err = fmt.Errorf("value is nil")
		return
	}

	// Add key if it does not exist
	smv, ok := lm.storeMap[k]
	if !ok {
		smv = newStoreMapValue()
		lm.storeMap[k] = smv
	}

	// Update value
	rw := *cm.GetMutex(k, &smv)
	rw.wLock()
	var oldValue []byte
	if smv.value != nil {
		oldValue = CopyByteArray(smv.value)
	}
	newValue := CopyByteArray(v)
	smv.value = v

	// Write log entry
	lm.addLogEntry(&pb.LogEntry{
		Tid:       proto.Int64(int64(tid)),
		EntryType: pb.LogEntry_UPDATE.Enum(),
		Key:       proto.String(string(k)),
		OldValue:  oldValue,
		NewValue:  newValue,
	})

	return
}

func (lm logManager) deleteValue(tid TransactionID, k Key) (err error) {
	cm, ok := lm.currMutexes[tid]
	if !ok {
		err = fmt.Errorf("transaction with ID %d is not currently running", tid)
	}
	smv, ok := lm.storeMap[k]
	if !ok {
		err = fmt.Errorf("key %s does not exist", k)
		return
	}

	// Delete key
	rw := *cm.GetMutex(k, &smv)
	rw.wLock()
	oldValue := CopyByteArray(smv.value)
	var newValue []byte
	delete(lm.storeMap, k)

	// Write log entry
	lm.addLogEntry(&pb.LogEntry{
		Tid:       proto.Int64(int64(tid)),
		EntryType: pb.LogEntry_UPDATE.Enum(),
		Key:       proto.String(string(k)),
		OldValue:  oldValue,
		NewValue:  newValue,
	})

	return
}

var lm logManager

func init() {
	lm = logManager{
		currMutexes: make(map[TransactionID]currentMutexesMap),
		storeMap:    make(map[Key]storeMapValue),
	}
}
