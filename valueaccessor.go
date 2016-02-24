package gostore

import (
	"time"
)

// valueAccessor provides an alternate interface for reading (and optionally,
// modifying) the Value of a key-mapped StoreMapValue. Transactions can request a
// valueAccessor through the logManager for either reading, or for reading/writing.
// This interface exists to regulate access to the Value, specifically disallow
// readers when there is a writer transaction, and vice-versa.
type valueAccessor struct {
	c           chan Value
	done        chan bool
	isWAccessor bool
}

func getRAccessor(smv storeMapValue) *valueAccessor {
	select {
	case smv.ping <- struct{}{}:
	default:
		initializeAccessorChan(smv)
	}
	return <-smv.rAccessorChan
}

func getWAccessor(smv storeMapValue) *valueAccessor {
	select {
	case smv.ping <- struct{}{}:
	default:
		initializeAccessorChan(smv)
	}
	return <-smv.wAccessorChan
}

func (va *valueAccessor) promoteToWAccessor(smv storeMapValue) *valueAccessor {
	if va.isWAccessor {
		return va
	}
	va.done <- true
	return getWAccessor(smv)
}

func connectAccessor(va *valueAccessor, smv storeMapValue, connDone chan bool) {
	v := smv.value
	for {
		select {
		case va.c <- v:
		case v = <-func() chan Value {
			if va.isWAccessor {
				return va.c
			}
			return nil
		}():
		case d := <-va.done:
			close(va.c)
			close(va.done)
			if va.isWAccessor && d {
				smv.value = v
			}
			connDone <- true
		}
	}
}

var accessorTimeout = time.Minute * 10

// initializeAccessorChan for a StoreMapValue starts up a goroutine that regulates
// the provision of read and write accessors to the StoreMapValue. Essentially, it
// behaves like a RWMutex.
func initializeAccessorChan(smv storeMapValue) {
	go func() {
		numReaders := 0
		connDone := make(chan bool)
		for {
			va := valueAccessor{make(chan Value), make(chan bool), false}
			select {
			case smv.wAccessorChan <- func() *valueAccessor {
				if numReaders == 0 { // only allowed if there are no read accessors
					return &va
				}
				select {}
			}():
				va.isWAccessor = true
				go connectAccessor(&va, smv, connDone)
				<-connDone // block since no one else can access while there is a write accessor
			case smv.rAccessorChan <- &va: // Read Access
				numReaders++
				va.isWAccessor = false
				go connectAccessor(&va, smv, connDone)
			case <-connDone: // Commit/Abort
				numReaders--
			case <-smv.ping:
			case <-func() <-chan time.Time {
				if numReaders == 0 { // only allowed if there are no accessors
					return time.After(accessorTimeout)
				}
				return nil
			}(): // Timeout
				break
			}
		}
		close(connDone)
	}()
	return
}
