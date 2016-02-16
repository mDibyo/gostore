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

// TransactionID is used to uniquely identify/represent a transaction
type TransactionID int64

// New Transaction creates a new transaction and returns its TransactionID.
func NewTransaction() (tid TransactionID) {
	return
}

// Commit commits and ends the transaction with this TransactionID.
func (tid TransactionID) Commit() (err error) {
	return
}

// Commit aborts and ends the transaction with this TransactionID.
func (tid TransactionID) Abort() {
	return
}

// Get retrieves the value of a key in transaction with this TransactionID.
func (tid TransactionID) Get(key Key) (value Value, err error) {
	return
}

// Set sets the value of a key in transaction with this TransactionID.
func (tid TransactionID) Set(key Key, value Value) (err error) {
	return
}

// Delete deletes a key in transaction with this TransactionID.
func (tid TransactionID) Delete(key Key) (err error) {
	return
}

// Get retrieves the value of a key in a new single-operation transaction.
func Get(key Key) (value Value, err error) {
	tid := NewTransaction()
	value, err = tid.Get(key)
	if err != nil {
		tid.Abort()
		return
	}
	err = tid.Commit()
	return
}

// Set sets the value of a key in a new single-operation transaction.
func Set(key Key, value Value) (err error) {
	tid := NewTransaction()
	if err = tid.Set(key, value); err != nil {
		tid.Abort()
		return
	}
	err = tid.Commit()
	return
}

// Delete deletes a key in a new single-operation transaction.
func Delete(key Key) (err error) {
	tid := NewTransaction()
	if err = tid.Delete(key); err != nil {
		tid.Abort()
		return
	}
	err = tid.Commit()
	return
}

func init() {
	lm = logManager{pb.Log{}, make(map[Key]StoreMapValue)}
}
