package gostore

import "sync"

// CopyByteArray returns a copy of src byte array
func CopyByteArray(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

// rwMutexWrapper is a thread-safe convenience wrapper for sync.RWMutex used in StoreMapValue.
type rwMutexWrapper struct {
	selfLock sync.Mutex    // Self Lock to synchronize lock and unlock operations.
	smvLock  *sync.RWMutex // the lock being wrapped.
	held     bool          // Whether the lock is held.
	wAllowed bool          // Whether writes are allowed.
}

func wrapRWMutex(l *sync.RWMutex) rwMutexWrapper {
	return rwMutexWrapper{smvLock: l}
}

func (rw *rwMutexWrapper) rLocked() (b bool) {
	rw.selfLock.Lock()
	b = rw.held && !rw.wAllowed
	rw.selfLock.Unlock()
	return
}

func (rw *rwMutexWrapper) wLocked() (b bool) {
	rw.selfLock.Lock()
	b = rw.held && rw.wAllowed
	rw.selfLock.Unlock()
	return
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
	rw.smvLock.RLock()
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
	rw.smvLock.RUnlock()
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
	rw.smvLock.Lock()
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
	rw.smvLock.Unlock()
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

func (rw *rwMutexWrapper) unlock() {
	rw.selfLock.Lock()
	defer rw.selfLock.Unlock()

	if !rw.held {
		return
	}

	if rw.wAllowed {
		rw.wUnlockUnsafe()
	} else {
		rw.rUnlockUnsafe()
	}
}
