package gostore

import (
	"github.com/mDibyo/utils/queue"
)

type doneChan chan struct{}

func newDoneChan(outChan chan struct{}, counter *int) doneChan {
	// TODO: Synchronize changing of counter
	*counter++
	dc := make(doneChan)
	go func() {
		<-dc
		*counter--
		outChan <- struct{}{}
	}()
	return dc
}

type accessChan chan bool

type conn struct {
	tid TransactionID // ID for the transaction trying to connect
	ac  accessChan    // the channel on which the transaction is listening
	dc  doneChan      // the channel on which the transaction sends when done
}

type rwAccessor struct {
	rConnChan chan *conn
	wConnChan chan *conn
	ping      chan struct{}
}

type accessorHandler func(*rwAccessor, Key) bool

func (a *rwAccessor) lazySetup() {
	select {
	case <-a.ping:
		return
	default:
	}

	done := make(chan struct{})
	numReaders, numWriters := 0, 0
	rWaiters := []*conn{}
	wWaiters := queue.Queue{}
	for {
		select {
		case a.ping <- struct{}{}: // Ping to ensure this routine is ready.
		case <-done: // Access closed. If possible, schedule new readers/writer.
			if numWriters > 0 {
				// Can not schedule new readers/writer.
				continue
			}

			if wWaiters.Len() == 0 {
				// No waiting writers. Schedule readers.
				for _, rConn := range rWaiters {
					rConn.dc = newDoneChan(done, &numReaders)
					rConn.ac <- true
				}
				rWaiters = []*conn{}
			} else if numReaders == 0 {
				wConn := wWaiters.Pop()
				wConn.(*conn).dc = newDoneChan(done, &numWriters)
				wConn.(*conn).ac <- true
			}
		case newRConn := <-a.rConnChan:
			// TODO: Perform deadlock detection
			rWaiters = append(rWaiters, newRConn)
		case newWConn := <-a.wConnChan:
			// TODO: Perform deadlock detection
			wWaiters.Push(newWConn)
		}
	}
}

func (a *rwAccessor) RAccess(c *conn) bool {
	a.lazySetup()
	a.rConnChan <- c
	return <-c.ac
}

func (a *rwAccessor) WAccess(c *conn) bool {
	a.lazySetup()
	a.wConnChan <- c
	return <-c.ac
}

func (a *rwAccessor) Release(c *conn) {
	c.dc <- struct{}{}
}

type heldAccessorsMap map[Key]*rwAccessor

type heldConnsMap map[Key]*conn

type LockManager struct {
	accessors     map[Key]rwAccessor                 // accessors for each key
	heldAccessors map[TransactionID]heldAccessorsMap // accessors for keys held by each transaction
	heldConns     map[TransactionID]heldConnsMap     // connections for keys held by each transaction
}

func NewLockManager() LockManager {
	return LockManager{
		make(map[Key]rwAccessor),
		make(map[TransactionID]heldAccessorsMap),
		make(map[TransactionID]heldConnsMap),
	}
}

func (lm *LockManager) accessor(k Key) *rwAccessor {
	a, ok := lm.accessors[k]
	if !ok {
		a = rwAccessor{}
		lm.accessors[k] = a
	}
	return &a
}
