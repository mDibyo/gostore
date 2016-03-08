# gostore

gostore allows for the storage of arbitrary data (as byte arrays) with string keys. During operation, 
the store is represented as a in-memory standard go map. Multiple keys can be added, updated or 
deleted in a single ‘transaction’.

Data is persisted through a log of operations. Updates associated with a transaction are pushed to 
disk before a transaction is committed. This ensures **Durability**. This is the only persistent record 
of data.

Consequently, while starting up, the present state of the store is constructed by reading in the log 
from disk, and replaying all entries in the log. Operations for transactions that were not committed 
(possibly due to a crash) are rolled back. This ensures **Atomicity** of transactions.

Concurrent transactions are allowed, and managed with read and write locks on each key-value pair. 
Multiple transactions can hold a read lock, but a transaction can hold a write lock only when no other 
transaction is holding either read or write locks on that key-value pair. Transactions acquire locks 
when they first need to read/modify an pair, and hold them until they are committed (Strict 2PL). In 
this way, **Isolation** of transactions is ensured.

Since there are no real constraints on the store, **Consistency** is trivially ensured.

