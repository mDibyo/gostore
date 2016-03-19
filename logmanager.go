/*
Package gostore implements a simple single-node log-based key-value
store. It supports multiple concurrent transactions through a set of
locks on values.
*/
package gostore

import (
	"flag"
	"fmt"
	"github.com/golang/protobuf/proto"
	pb "github.com/mDibyo/gostore/pb"
	"io/ioutil"
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

func newStoreMapValue() *storeMapValue {
	return &storeMapValue{
		rAccessorChan: make(chan *valueAccessor),
		wAccessorChan: make(chan *valueAccessor),
		ping:          make(chan struct{}),
	}
}

// TransactionID is used to uniquely identify/represent a transaction.
type TransactionID int64

type storeMap map[Key]*storeMapValue

func (sm storeMap) storeMapValue(k Key, addIfNotExist bool) (smv *storeMapValue, err error) {
	smv, ok := sm[k]
	if ok {
		return
	}
	if !addIfNotExist {
		return smv, fmt.Errorf("key %s does not exist.", k)
	}

	smv = newStoreMapValue()
	sm[k] = smv
	return
}

type currentMutexesMap map[Key]*rwMutexWrapper

func (cm currentMutexesMap) getWrappedRWMutex(k Key, smv *storeMapValue) *rwMutexWrapper {
	if rw, ok := cm[k]; ok {
		return rw
	}
	_rw := wrapRWMutex(&smv.lock)
	cm[k] = &_rw
	return &_rw
}

var logFileFmt = "%012d_%012d.log"

type logManager struct {
	log            pb.Log                              // the log of transaction operations
	logDir         string                              // the directory in which log is stored
	logLock        sync.Mutex                          // lock to synchronize access to the log
	nextLSN        int                                 // the LSN for the next log entry
	nextLSNToFlush int                                 // the LSN of the next log entry to be flushed
	nextTID        TransactionID                       // the Transaction ID for the next transaction
	currMutexes    map[TransactionID]currentMutexesMap // the mutexes held currently by running transactions
	store          storeMap                            // the master copy of the current state of the store
}

func newLogManager(ld string) (lm *logManager, err error) {
	lm = &logManager{}
	lm.logDir = ld
	if lm.logDir == "" {
		lm.logDir = "./data"
	}
	lm.currMutexes = make(map[TransactionID]currentMutexesMap)
	lm.store = make(storeMap)

	// Retrieve old logs if they exist
	err = lm.retrieveLog()

	// Replay log over storeMap
	for _, e := range lm.log.Entry {
		tid := TransactionID(*e.Tid)
		switch *e.EntryType {
		case pb.LogEntry_BEGIN:
			lm.currMutexes[tid] = make(currentMutexesMap)
		case pb.LogEntry_UPDATE:
			fallthrough
		case pb.LogEntry_UNDO:
			lm.updateStoreMapValue(lm.currMutexes[tid], Key(*e.Key), Value(CopyByteArray(e.NewValue)))
		case pb.LogEntry_COMMIT:
		case pb.LogEntry_ABORT:
		case pb.LogEntry_END:
			delete(lm.currMutexes, tid)
		}
	}

	// Abort incomplete transactions
	for tid, _ := range lm.currMutexes {
		lm.abortTransaction(tid)
	}

	return
}

func (lm *logManager) addLogEntry(e *pb.LogEntry) {
	lm.logLock.Lock()
	defer lm.logLock.Unlock()

	entries := &lm.log.Entry
	e.Lsn = proto.Int64(int64(lm.nextLSN))
	*entries = append(*entries, e)
	lm.nextLSN++
}

func (lm *logManager) retrieveLog() (err error) {
	files, err := ioutil.ReadDir(lm.logDir)
	if err != nil {
		return fmt.Errorf("could not retrieve old logs: %v", err)
	}

	for _, file := range files {
		if !file.IsDir() {
			var startLSN, endLSN = -1, -1
			_, err = fmt.Sscanf(file.Name(), logFileFmt, &startLSN, &endLSN)
			if err != nil {
				continue
			}
			if startLSN != lm.nextLSN || endLSN < startLSN {
				err = fmt.Errorf("log file %s was not in the expected format", file.Name())
				break
			}
			filename := fmt.Sprintf("%s/%s", lm.logDir, file.Name())
			data, err := ioutil.ReadFile(filename)
			if err != nil {
				err = fmt.Errorf("could not read log file %s: %v", filename, err)
				break
			}
			if err = proto.UnmarshalMerge(data, &lm.log); err != nil {
				err = fmt.Errorf("could not unmarshal log file %s: %v", filename, err)
				break
			}
			lm.nextLSN = len(lm.log.Entry)
			if nextLSN := endLSN + 1; nextLSN != lm.nextLSN {
				err = fmt.Errorf("log file %s did not have the right number of entries", filename)
				break
			}
		}
	}
	lm.nextLSNToFlush = lm.nextLSN
	return err
}

func (lm *logManager) flushLog() error {
	lm.logLock.Lock()
	defer lm.logLock.Unlock()

	entries := lm.log.GetEntry()
	logToFlush := &pb.Log{
		Entry: entries[lm.nextLSNToFlush:],
	}
	data, err := proto.Marshal(logToFlush)
	if err != nil {
		return fmt.Errorf("error while marshalling log to be flushed: %v", err)
	}
	filename := fmt.Sprintf(logFileFmt, lm.nextLSNToFlush, lm.nextLSN-1)
	if err := ioutil.WriteFile(fmt.Sprintf("%s/%s", lm.logDir, filename), data, 0644); err != nil {
		return fmt.Errorf("error while writing out log: %v", err)
	}
	lm.nextLSNToFlush = lm.nextLSN
	return nil
}

func (lm *logManager) nextTransactionID() TransactionID {
	lm.nextTID++
	return lm.nextTID - 1
}

func (lm *logManager) beginTransaction(tid TransactionID) {
	lm.currMutexes[tid] = make(currentMutexesMap)
	lm.addLogEntry(&pb.LogEntry{
		Tid:       proto.Int64(int64(tid)),
		EntryType: pb.LogEntry_BEGIN.Enum(),
	})
}

func (lm *logManager) getValue(tid TransactionID, k Key) (Value, error) {
	cm, ok := lm.currMutexes[tid]
	if !ok {
		return nil, fmt.Errorf("transaction with ID %d is not currently running", tid)
	}
	smv, err := lm.store.storeMapValue(k, false)
	if err != nil {
		return nil, fmt.Errorf("could not retrieve value: %v", err)
	}

	rw := cm.getWrappedRWMutex(k, smv)
	rw.rLock()
	return smv.value, nil
}

func (lm *logManager) updateStoreMapValue(cm currentMutexesMap, k Key, v Value) (oldValue, newValue []byte, err error) {
	smv, err := lm.store.storeMapValue(k, true)
	if err != nil {
		return nil, nil, fmt.Errorf("could not retrieve value: %v", err)
	}

	rw := cm.getWrappedRWMutex(k, smv)
	rw.wLock()
	if smv.value != nil {
		oldValue = CopyByteArray(smv.value)
	}
	if v != nil {
		smv.value = v
		newValue = CopyByteArray(v)
	} else {
		delete(lm.store, k)
	}

	return
}

func (lm *logManager) updateValue(tid TransactionID, k Key, v Value) error {
	cm, ok := lm.currMutexes[tid]
	if !ok {
		return fmt.Errorf("transaction with ID %d is not currently running.", tid)
	}
	oldValue, newValue, err := lm.updateStoreMapValue(cm, k, v)
	if err != nil {
		return err
	}

	// Write log entry
	lm.addLogEntry(&pb.LogEntry{
		Tid:       proto.Int64(int64(tid)),
		EntryType: pb.LogEntry_UPDATE.Enum(),
		Key:       proto.String(string(k)),
		OldValue:  oldValue,
		NewValue:  newValue,
	})

	return nil
}

func (lm *logManager) setValue(tid TransactionID, k Key, v Value) error {
	if v == nil {
		return fmt.Errorf("value is nil.")
	}
	return lm.updateValue(tid, k, v)
}

func (lm *logManager) deleteValue(tid TransactionID, k Key) error {
	_, err := lm.store.storeMapValue(k, false)
	if err != nil {
		return err
	}
	return lm.updateValue(tid, k, nil)
}

func (lm *logManager) commitTransaction(tid TransactionID) error {
	cm, ok := lm.currMutexes[tid]
	if !ok {
		return fmt.Errorf("transaction with ID %d is not currently running", tid)
	}

	// Write out COMMIT and END log entries
	lm.addLogEntry(&pb.LogEntry{
		Tid:       proto.Int64(int64(tid)),
		EntryType: pb.LogEntry_COMMIT.Enum(),
	})

	lm.addLogEntry(&pb.LogEntry{
		Tid:       proto.Int64(int64(tid)),
		EntryType: pb.LogEntry_END.Enum(),
	})

	// Flush out log
	if err := lm.flushLog(); err != nil {
		return fmt.Errorf("error while flushing log: %v", err)
	}

	// Release all locks and remove from current transactions
	for _, rw := range cm {
		rw.unlock()
	}
	delete(lm.currMutexes, tid)
	return nil
}

func (lm *logManager) abortTransaction(tid TransactionID) (err error) {
	cm, ok := lm.currMutexes[tid]
	if !ok {
		err = fmt.Errorf("transaction with ID %d is not currently running", tid)
		return
	}

	// Write out ABORT entry
	lm.addLogEntry(&pb.LogEntry{
		Tid:       proto.Int64(int64(tid)),
		EntryType: pb.LogEntry_ABORT.Enum(),
	})

	// Undo updates (and write log entries)
	entries := &lm.log.Entry
	iterateEntries := (*entries)[:]
iterate:
	for i := len(iterateEntries) - 1; i >= 0; i-- {
		e := iterateEntries[i]
		if *e.Tid == int64(tid) {
			switch *e.EntryType {
			case pb.LogEntry_UPDATE: // Undo UPDATE records
				oldValue, newValue, err := lm.updateStoreMapValue(cm, Key(*e.Key), Value(e.OldValue))
				if err != nil {
					return err
				}
				lm.addLogEntry(&pb.LogEntry{
					Tid:       proto.Int64(int64(tid)),
					EntryType: pb.LogEntry_UNDO.Enum(),
					Key:       e.Key,
					OldValue:  oldValue, // e.NewValue
					NewValue:  newValue, // e.OldValue
					UndoLsn:   e.Lsn,
				})
			case pb.LogEntry_BEGIN: // Stop when BEGIN record is reached
				break iterate
			}
		}
	}

	lm.addLogEntry(&pb.LogEntry{
		Tid:       proto.Int64(int64(tid)),
		EntryType: pb.LogEntry_END.Enum(),
	})

	// Flush out log
	lm.flushLog()

	// Release all locks and remove from current transactions
	for _, rw := range cm {
		rw.unlock()
	}
	delete(lm.currMutexes, tid)
	return
}

var lmInstance logManager

func init() {
	logDir := flag.String("logDir", "", "the directory in which log files will be stored")
	flag.Parse()
	if lmInstancePtr, err := newLogManager(*logDir); err != nil {
		panic(err)
	} else {
		lmInstance = *lmInstancePtr
	}
}
