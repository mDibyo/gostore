package gostore

import (
	"sync"
)

type TransactionID int64

// New Transaction creates a new transaction and returns its TransactionID.
func NewTransaction() (tid TransactionID) {
	return
}

// Commit commits and ends the transaction with this TransactionID.
func (tid TransactionID) Commit() {
	return
}

// Commit aborts and ends the transaction with this TransactionID.
func (tid TransactionID) Abort() {
	return
}

// Get retrieves the value of a key in transaction with this TransactionID.
func (tid TransactionID) Get(key string) (value []byte, err error) {
	return
}

// Set sets the value of a key in transaction with this TransactionID.
func (tid TransactionID) Set(key string, value []byte) (err error) {
	return
}

// Delete deletes a key in transaction with this TransactionID.
func (tid TransactionID) Delete(key string) (err error) {
	return
}

// Get retrieves the value of a key in a new single-operation transaction.
func Get(key string) (value []byte, err error) {
	tid := NewTransaction()
	value, err = tid.Get(key)
	if err != nil {
		tid.Abort()
		return
	}
	tid.Commit()
	return
}

// Set sets the value of a key in a new single-operation transaction.
func Set(key string, value []byte) (err error) {
	tid := NewTransaction()
	if err = tid.Set(key, value); err != nil {
		tid.Abort()
		return
	}
	tid.Commit()
	return
}

// Delete deletes a key in a new single-operation transaction.
func Delete(key string) (err error) {
	tid := NewTransaction()
	if err = tid.Delete(key); err != nil {
		tid.Abort()
		return
	}
	tid.Commit()
	return
}
