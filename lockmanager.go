package gostore

import (
	"github.com/mDibyo/utils/queue"
	"sync"
)

type rwMutex struct {
	selfMutex  sync.Mutex
	valueMutex sync.RWMutex
	accessors  []TransactionID
}

func (rw *rwMutex) rLockedUnsafe() bool {
	return len(rw.accessors) > 1
}

func (rw *rwMutex) wLockedUnsafe() bool {
	return rw.accessors[0] != 0
}

func (rw *rwMutex) lockedUnsafe() bool {
	return rw.rLockedUnsafe() || rw.wLockedUnsafe()
}

type doneChan chan TransactionID

type accessChan chan doneChan

type connection struct {
	tid TransactionID // ID for the transaction trying to connect
	ac  accessChan    // the channel on which the transaction is listening
}

type rwAccessor struct {
	rConnChan chan connection
	wConnChan chan connection
	ping      chan struct{}
}

type accessorHandler func(*rwAccessor, Key) bool

func (rw *rwAccessor) setup() {
	select {
	case <-rw.ping:
		return
	default:
	}

	done := make(chan struct{})
	writing := false
	numReaders := 0
	rWaiters := []*connection{}
	wWaiters := queue.Queue{}
	for {
		select {
		case rw.ping <- struct{}{}: // Ping to ensure this routine is ready.
		case <-done: // Access closed. If possible, schedule new readers/writer.
			if writing {
				// Can not schedule new readers/writer.
				continue
			}

			if wWaiters.Len() == 0 {
				// No waiting writers. Schedule readers.
				for _, rConn := range rWaiters {
					numReaders++
					// TODO: Set up accessChan
					rConn.ac <- make(doneChan)
				}
				rWaiters = []*connection{}
			} else if numReaders == 0 {
				wConn := wWaiters.Pop()
				// TODO: Set up accessChan
				wConn.(*connection).ac <- make(doneChan)
			}
		case newRConn := <-rw.rConnChan:
			rWaiters = append(rWaiters, &newRConn)
		case newWConn := <-rw.wConnChan:
			wWaiters.Push(newWConn)
		}
	}
}

type heldMutexesMap map[Key]*rwMutex

type heldAccessorsMap map[Key]*rwAccessor

type LockManager struct {
	mutexes       map[Key]rwMutex                    // mutexes for every key
	accessors     map[Key]rwAccessor                 // accessors for each key
	heldMutexes   map[TransactionID]heldMutexesMap   // mutexes for keys held by each transaction
	heldAccessors map[TransactionID]heldAccessorsMap // accessors for keys held by each transaction
}

func NewLockManager() LockManager {
	return LockManager{
		make(map[Key]rwMutex),
		make(map[Key]rwAccessor),
		make(map[TransactionID]heldMutexesMap),
		make(map[TransactionID]heldAccessorsMap),
	}
}

func (lm *LockManager) mutex(k Key) *rwMutex {
	rw, ok := lm.mutexes[k]
	if !ok {
		rw = rwMutex{}
		lm.mutexes[k] = rw
	}
	return &rw
}

// RLocked returns whether K is read-locked.
func (lm *LockManager) RLocked(k Key) bool {
	rw := lm.mutex(k)
	rw.selfMutex.Lock()
	defer rw.selfMutex.Unlock()
	return rw.rLockedUnsafe()
}

// WLocked returns whether K is write-locked.
func (lm *LockManager) WLocked(k Key) bool {
	rw := lm.mutex(k)
	rw.selfMutex.Lock()
	defer rw.selfMutex.Unlock()
	return rw.wLockedUnsafe()
}

// Locked returns whether K is locked at all.
func (lm *LockManager) Locked(k Key) bool {
	rw := lm.mutex(k)
	rw.selfMutex.Lock()
	defer rw.selfMutex.Unlock()
	return rw.lockedUnsafe()
}

func (lm *LockManager) willDeadlockUnsafe(rw *rwMutex, k Key) bool {
	if rw.lockedUnsafe() {
		for _, a := range rw.accessors {
			if a == 0 {
				continue
			}
			hm := lm.heldMutexes[a]
			for otherKey, otherRW := range hm {
				if otherKey == k {
					return true
				}
				otherRW.selfMutex.Lock()
				if lm.willDeadlockUnsafe(otherRW, k) {
					return true
				}
				otherRW.selfMutex.Unlock()
			}
		}
	}
	return false
}

// RLock read-locks K for Transaction with ID TID.
func (lm *LockManager) RLock(tid TransactionID, k Key) bool {
	rw := lm.mutex(k)

	rw.selfMutex.Lock()
	defer rw.selfMutex.Unlock()
	if lm.willDeadlockUnsafe(rw, k) {
		return false
	}

	rw.valueMutex.RLock()
	rw.accessors = append(rw.accessors, tid)
	return true
}
