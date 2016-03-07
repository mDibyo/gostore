package gostore

// Transaction is an atomic operation or set of operations on the store.
type Transaction struct {
	tid TransactionID
}

// New Transaction creates a new transaction and returns it.
func NewTransaction() Transaction {
	t := Transaction{lmInstance.nextTransactionID()}
	lmInstance.beginTransaction(t.tid)
	return t
}

// Commit commits and ends Transaction.
func (t Transaction) Commit() (err error) {
	return lmInstance.commitTransaction(t.tid)
}

// Commit aborts and ends Transaction.
func (t Transaction) Abort() (err error) {
	return lmInstance.abortTransaction(t.tid)
}

// Get retrieves the value of a key in Transaction.
func (t Transaction) Get(key Key) (value Value, err error) {
	return lmInstance.getValue(t.tid, key)
}

// Set sets the value of a key in Transaction.
func (t Transaction) Set(key Key, value Value) (err error) {
	return lmInstance.setValue(t.tid, key, value)
}

// Delete deletes a key in Transaction.
func (t Transaction) Delete(key Key) (err error) {
	return lmInstance.deleteValue(t.tid, key)
}

// Get retrieves the value of a key in a new single-operation transaction.
func Get(key Key) (value Value, err error) {
	t := NewTransaction()
	value, err = t.Get(key)
	if err != nil {
		t.Abort()
		return
	}
	err = t.Commit()
	return
}

// Set sets the value of a key in a new single-operation transaction.
func Set(key Key, value Value) (err error) {
	t := NewTransaction()
	if err = t.Set(key, value); err != nil {
		t.Abort()
		return
	}
	err = t.Commit()
	return
}

// Delete deletes a key in a new single-operation transaction.
func Delete(key Key) (err error) {
	t := NewTransaction()
	if err = t.Delete(key); err != nil {
		t.Abort()
		return
	}
	err = t.Commit()
	return
}
