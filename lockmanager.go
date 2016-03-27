package gostore

import "sync"

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

type heldMutexesMap map[Key]*rwMutex

type LockManager struct {
	mutexes     map[Key]rwMutex                  // mutexes for every key
	heldMutexes map[TransactionID]heldMutexesMap // mutexes for keys held by each transaction
}

func NewLockManager() LockManager {
	return LockManager{
		make(map[Key]rwMutex),
		make(map[TransactionID]heldMutexesMap),
	}
}

func (lm *LockManager) mutex(k Key) (rw *rwMutex) {
	rw, ok := lm.mutexes[k]
	if !ok {
		rw = &rwMutex{}
		lm.mutexes[k] = rw
	}
	return
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
			if a == nil {
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
