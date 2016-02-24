package gostore

// TransactionID is used to uniquely identify/represent a transaction
type TransactionID int64

var nextTid TransactionID = 0

// New Transaction creates a new transaction and returns its TransactionID.
func NewTransaction() (tid TransactionID) {
	tid = nextTid
	lmInstance.beginTransaction(tid)
	nextTid++
	return
}

// Commit commits and ends the transaction with this TransactionID.
func (tid TransactionID) Commit() (err error) {
	return lmInstance.commitTransaction(tid)
}

// Commit aborts and ends the transaction with this TransactionID.
func (tid TransactionID) Abort() (err error) {
	return lmInstance.abortTransaction(tid)
}

// Get retrieves the value of a key in transaction with this TransactionID.
func (tid TransactionID) Get(key Key) (value Value, err error) {
	return lmInstance.getValue(tid, key)
}

// Set sets the value of a key in transaction with this TransactionID.
func (tid TransactionID) Set(key Key, value Value) (err error) {
	return lmInstance.setValue(tid, key, value)
}

// Delete deletes a key in transaction with this TransactionID.
func (tid TransactionID) Delete(key Key) (err error) {
	return lmInstance.deleteValue(tid, key)
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
