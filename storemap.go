/*
Package gostore implements a simple single-node log-based key-value
store. It supports multiple concurrent transactions through a set of
locks on values.
*/
package gostore

import (
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

type logManager struct {
	log      pb.Log
	storeMap map[Key]StoreMapValue
}

var lm logManager

var currentTid int64 = 0



func init() {
	lm = logManager{pb.Log{}, make(map[Key]StoreMapValue)}
}
