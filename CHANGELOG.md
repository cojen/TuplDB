Changelog
=========

v1.9.0 (2025-03-04)
------

* Bug fixes:
  * Fix query sorting against columns which are ordered descending or null low.
  * Add special checks for handling header corruption.
  * Throw exceptions when deleting files, except when the delete doesn't need to succeed.
  * Tests and (minor) fixes for opening the database multiple times.
  * Fix View Query implementation of argumentCount.
  * The generated join classes should only use the Nullable annotation when the joined column
    is part of an outer join.
  * Workaround an unknown bug in which the file lock close method throws an exception
    indicating that the underlying channel has been closed, when in fact it should not have
    been closed yet.
  * Strip off IPv6 scoped address from replication group members.
  * Prevent double rollback if local leadership is lost in the middle of writing a redo commit
    operation.
  * Fix a potential race condition which could delete a segment file after it's brought back
    into service.
  * Fix a bug when extending a term causes the contiguous range to extend when it shouldn't.
  * Force a checkpoint when converting to/from replicated mode.
  * Drop direct use of FileChannel because it closes down when the thread is interrupted.
  * Sync should work even when opened in read-only mode.
  * Fix sync race conditions when checkpoint is running.
  * The deleteAll method should operate within a transaction scope.

* Breaking changes:
  * Depends on Java 22 and switched to the Java FFM API. Direct page access mode is now always
    enabled, and disabling it has no effect. This change isn't really expected to be
    "breaking", but it's a major change. The FFM API is slightly slower compared to using the
    Unsafe class directly, and so TuplDB will attempt to use one of the Unsafe classes
    automatically by default, falling back to the FFM API when necessary.
  * Swapped checksum and encryption order.
  * Remove the PageArray.open method. It was only needed by a test.
  * Rename JoinedPageArray to SpilloverPageArray.

* New features:
  * Support complex query expressions.
  * Support derived queries.
  * Define newScanner and newUpdater methods which accept an initial row instance.
  * Define an optional Row interface.
  * Add a method for processing all the set columns of a row.
  * Allow Mapper to indicate if it performs any filtering.
  * Add rowType and argumentCount methods to the Query interface.
  * Notify when allocation using large pages didn't work.
  * Attempt to restore a corrupt header from one of the copies.
  * Define an explicit exception for checksum mismatches.
  * Support accessing join columns via dotted path names.
  * Add a Table method which returns the join identity.
  * Add leader state and failover JMX operations.
  * Remote API relies on auto-dispose features of Dirmi.
  * Give replicator threads a useful name.
  * Add a method to rebuild a database.
  * Support large pages when using Windows.
  * Make Transaction interface extend Closeable.
  * Add a map method for performing column conversions.
  * Allow enter() to be called on bogus transaction considering that exit() is already allowed.
  * Allow non-deriving expressions in mapped, aggregate, and grouped table queries.
  * Add the ability to concatenate tables together.
  * Add a Table.hasPrimaryKey method.
  * Add a Table.distinct view method.

* Performance improvements:
  * Don't hold the node latch the whole time when verifying large fragmented values.
  * A temporary database doesn't need the checkpointer running.
  * Don't issue a checkpoint when finishing an index build if automatic checkpointing isn't
    enabled.
  * Make remote decoding a bit more efficient by not allocating a temporary byte array.
  * Process pending transactions batches, reducing the number of latch acquisitions.
  * Fix a performance issue when leadership is lost during a checkpoint, caused by massive
    amount of hash collisions.
  * Speed up verification by using multiple threads.
  * Reduce the amount of time the Parker spins (and yields) before fully parking. Overall
    performance is just as good and CPU load is lower.
  * Skip over large value gaps when verifying or copying them.


v1.8.0 (2024-03-04)
------

* Bug fixes:
  * Fixed a rare deadlock in the recovery subsystem when applying an index rename operation.
  * Don't delete corrupt log files when opening in read-only mode.
  * Don't attempt to checkpoint a read-only database, which will fail.
  * Various fixes to the generated code for decoding rows from their binary representation.
  * Various fixes to the table predicate locking subsystem.
  * Index tables are now cached by their primary table row type to prevent conflicts when
    multiple versions of the primary row type class exist.
  * Ensure that the trigger for managing secondary index is installed when opening a table.
  * Fixed deletion of tables which have secondary indexes.
  * Prevent secondary indexes from being used when the corresponding table is closed.
  * Abort in-progress secondary scans when closing a table.
  * Fixed a deadlock when table indexes change.
  * Fixed a concurrent modification exception which can occur during a secondary index backfill.
  * The generated row comparator classes now honor columns which have null low ordering.
  * The generated row predicate classes check if a column is set when being accessed, throwing
    an exception instead of silently accepting null as valid.
  * The generated row predicate classes now support comparisons between columns with different
    types.
  * Reduced the amount of cached direct byte buffers that can accumulate due to the use of
    thread-local variables. A specialized cache is used instead.
  * Promptly shutdown any table-related background tasks when the database is closed.
  * The verifier doesn't attempt to check nodes which are in an ephemeral split state.
  * Fixed local host acquisition with respect to ipv6, as used by the replication subsystem.
  * Calling `View.isEmpty` now checks if the underlying index is closed, throwing a
    `ClosedIndexException` if so.

* Breaking changes:
  * The `load`, `insert`, `replace`, `update`, `merge`, and `delete` methods now throw an
    exception instead of returning a boolean value. The original methods are supported in the
    form of newly added "try" variants: `tryLoad`, `tryInsert`, `tryReplace`, `tryUpdate`,
    `tryMerge`, and `tryDelete`.
  * The `Table` methods for obtaining `QueryPlan` instances have been moved to the new `Query`
    interface.
  * Removed some classes and methods in the io package that cannot easily be adapted to work
    with FFM/Panama, but they're not needed anyhow.
  * `LatchCondition` is now an inner class, `Latch.Condition`.

* New features:
  * Defined a new `Query` interface which directly represents a runnable query. It can be used
    as an alternative to passing query strings to the `Table` class all the time.
  * The `Query` interface has a `deleteAll` method, which provides new functionality not
    available in the `Table` interface.
  * Added a `Table.map` method for supporting custom row filtering and transforming.
  * Added a `Table.aggregate` method for supporting row aggregation.
  * Added a `Table.group` method for supporting custom row aggregation and window functions.
  * Added a `Table.join` method which performs a relational join against two tables.
  * Added a `Table.view` method which returns a table which restricts the set of rows to those
    bounded by a query.
  * Added a `Table.anyRows` method.
  * Added new `Table` methods which act upon row instances: `cleanRow` and `isSet`.
  * The `Scanner` interface permits a null step row instead of throwing an exception. When null
    is passed to the `step` method, a new row instance is created automatically. Likewise, the
    `delete` and `update` methods of the `Updater` interace permit a null row to be passed in.
  * Added a `Transaction.rollback` method, which unlike the `exit` method, doesn't exit the
    current transaction scope.
  * The `Database.verify` method now checks for duplicate page identifiers in the free list.

* Performance improvements:
  * Binary search has been optimized by performing 8-byte comparisons instead of 1-byte
    comparisons.
  * The primary database cache now uses transparent huge pages, on Linux only.
  * Support the random access flag when opening mapped files.
  * When selecting an index for a full scan of a table, natural ordering of the index is now
    considered.
  * When two indexes are both covering, favor the one whose natural order matches the requested
    order before considering the total number of index columns.


v1.7.0 (2023-03-04)
------

* Bug fixes:
  * Don't truncate extra pages if there were any issues when opening the file. It might
    indicate a configuration error which will be detected later as indexes are opened up.
  * When replicated, cannot start checkpoints or tasks that depend on checkpoints until after
    the ReplController is ready. A race condition would sometimes cause a NullPointerException
    on the first checkpoint and panic the database.
  * Fixed handling of file mappings during a remap operation which caused live mappings to be
    closed.
  * When waiting for replication to catch up, double check the commit position at most
    once. Without this check, the database would start up much sooner, allowing access to stale
    data.
  * Queries no longer select table indexes which aren't available.
  * Guard against table indexes from being concurrently dropped when scanning with a null
    transaction.
  * Optimizations and fixes for table row predicate locks.
  * Numerous query plan optimizations and fixes.

* Breaking changes:
  * Removed the EntryConsumer, EntryFunction, RowScanner, and RowUpdater interfaces.
  * Repurposed the Scanner and Updater interfaces to only work with tables.
  * Reading from a closed/deleted index now throws ClosedIndexException or DeletedIndexException.
  * ClosedIndexException now extends DatabaseException directly.
  * Introduced an internal table definition version number, which can cause issues when opening
    up tables which were created in earlier releases.
  * Table query parameters follow a one-based argument numbering scheme now instead of a
    zero-based scheme. It's now consistent with JDBC.
  * Removed the ':' separator from the query specification.
  * Removed the Table view methods.

* New features:
  * Support remote database access.
  * Support table query ordering, which selects an appropriate index or performs a sort.
  * Support null ordered low behavior for index columns and query projections. The default is
    still null ordered high when unspecified.
  * Added a table close method.
  * Added more JMX operations, and enabled notifications.


v1.6.0 (2022-05-18)
------
* Fix race condition which can cause the JVM to crash when the database is closed.
* Fix deadlock during compaction caused by releasing the wrong node latch.
* Fix improper lock release when writing cache primer.
* Fix improper lock release/acquire when when opening indexes.
* Fix BoundedCursor such that it doesn't observe uncommitted deletes when initially positioning it.
* Fix possible reporting of negative cursor count in the database stats.
* Fix safety of using updaters that require lock mode upgrades.
* Improve utility of the suspendCheckpoints method by waiting for an in-progress checkpoint to
  complete.
* Don't reset checkpoint duration stat when thresholds aren't met.
* Depends on Java 17.
* Add an API for supporting relational collections of persistent rows.

v1.5.3.3 (2021-10-03)
--------
* Ensure that index passed to verification observer is unmodifiable when called from an
  unmodifiable index.

v1.5.3.2 (2021-09-13)
--------
* Fix storage leaks caused by fragmented keys.
* Fix NPE when obtaining stats on a closed database.
* Compaction scan now finds fragmented keys.
* Suppress uncaught exception from background sorter thread when database is closed.

v1.5.3.1 (2021-08-30)
--------
* Fix potential race condition when concurrently deleting and truncating the checkpoint undo log.

v1.5.3 (2021-08-09)
------
* Avoid holding global commit lock when deleting an index.
* Add a method to check if transaction is bogus.

v1.5.2 (2021-07-24)
------
* Newly entered transaction scopes always select the UPGRADABLE_READ lock mode instead of
  inheriting the mode from the parent scope.
* Use stronger random number for database id.

v1.5.1 (2021-07-10)
------
* Add more utilities for decoding short integers and unsigned integers.
* Ignore interrupts during critical operations, preventing database panics.
* Prune empty nodes when deleting ghost entries. If this isn't done, then an index whose
  entries have been fully deleted can claim to be non-empty.

v1.5.0.1 (2021-06-08)
--------
* Support fully mapped databases when the page size doesn't match the OS page size.
* Use a faster hash implementation for the lock manager.
* Fix edge case where a non-stored database would prematurely run out of space.

v1.5.0 (2021-05-05)
------

* Depends on Java 12 and defines a module: org.cojen.tupl.
* Fix page leak when deleting temporary trees which were created by the Sorter.
* Fix data loss when replicating values using the ValueAccessor API.
* Fix replication ABA race condition which caused a blank range of data (zeros) to appear as
  valid to the replica, when a term is truncated and extended.
* Fix recovery and cleanup of registered cursors.
* Fix handling of snapshot/restore with custom data page array.
* Fix replication segments which are fully truncated. They must be untruncated when term is later
  extended.
* Fix potential race condition with latch condition signal.
* Fix for lost exclusive lock acquisitions caused by delete short-circuit.
* Fix which caused replica cursor registration to be lost following a checkpoint and recovery.
* Fix undelete of large key and value which didn't cleanup the trash.
* Fix checkpoint resumption after failure.
* Fix race condition when open an index while it's being renamed.
* Fix rare NPE caused by race condition in the replication engine.
* Fix writer starvation issue when there are a lot of active readers.
* Fix in replication hole fill request handling which dropped them too soon, leading to a flood
  of duplicate requests.
* Fix potential deadlock in the count and skip methods.
* Fix where the cache primer creation task could stall behind a long running cursor.
* Fix trashed tree race condition with recovery of a non-replicated database.
* Fix premature lock acquisition bug caused by a timeout.
* Fix potential deadlocks when opening a tree.
* Fix extending and truncating fragmented values when using a large page size.
* Fix off by one error when storing a 8193 byte value into a large page.
* Fix potential deadlock when throwing CacheExhaustedException or DatabaseFullException.
* Various fixes for the database compaction feature.
* Replace "unsafe" usage with VarHandles.
* Guard against a flood of duplicate hole fill requests. De-duplicate them and handle them in
  at most one thread per remote peer.
* Support for inclusive/exclusive ranges with count and random methods, and fix double wrapping
  count defect.
* Wait for replication recovery when starting up, and wait for the local member to become the
  leader if it's the only group member which can become the leader.
* Speed up redo log recovery by using multiple threads.
* Reduce memory requirements when running the test suite.
* Simplify TransactionHandler interface, and unify it to some extent with RecoveryHandler.
* Update dependencies to support Java 11.
* WriteFailureException no longer extends DatabaseFullException.
* Optimize update and delete by not always increasing the node garbage size.
* Handle edge cases where a latches wouldn't get released when an exception was thrown.
* Define a default Cursor.close method.
* Added socket factory support to replicator.
* Improve replication election stability.
* Reduce the number of latch acquisition retries when storing entries (performance).
* Thread safety fixes in replication layer during checkpoints and leadership changes.
* Introduce background syncCommit task, which allows peers to compact their log even when the
  database isn't running checkpoints.
* Support LZ4 compression with replication peer restore.
* When refreshing the replication peer set, must disconnect old peers before connecting new
  ones, to prevent possible false address collisions.
* Reduce object allocations and copies within the replication subsystem.
* Provide public access to the commit lock.
* Added a View.isEmpty method.
* Free up more memory when closing the database in case something refers to the instance for a
  long time. Usually this is caused by a thread local reference somewhere.
* Remove spare page pool and keep a spare page within each node group instead. This eliminates
  some contention when many threads are simultaneously compacting pages.
* Added support for proxying writes from the replication leader via a peer. Experimental for now.
* Added ability to check if current member is the leader.
* Boost the size of the replication writer cache when creating non-contiguous writers, to
  ensure that they don't get lost. When they're lost, additional hole filling requests are
  performed.
* Added non-blocking replication read support.
* Added more replication roles and refine exitsing ones.
* Redesign custom transaction handler such that multiple named handlers can be installed.
* Redesign prepare transaction support to match custom handler and to fix failover defects.
* Include database stats when throwing a cache exhausted exception.
* Added a convenience method to set the cache size.
* Attempt to reduce write stalls when draining the PageQueue by relying on the main database
  cache to postpone the write until the next checkpoint.
* LatchCondition now unparks the signaled waiter as the latch is released. Earlier behavior
  could result in extra context switches (thundering herd).
* Latching performance improvements when parking/unparking threads.
* Don't dirty a node when insert/replace/update returns false, reducing unnecessary writes.
* Code coverage improvements and fixes.
* Support conversion to/from replicated mode.
* Performance improvements for pending transactions.
* Support pending transaction notification with a new CommitCallback interface.
* Automatically pre-touch direct access pages when creating the cache.
* Support limited read-only mode which doesn't persist any changes.
* File I/O operations are no longer affected by thread interrrupts. The database remains open.
* Added support for page-level compression and checksums.
* Support clean database shutdown triggered by clean JVM exit.
* Added support for asynchronous lock acquisition.
* Added basic JMX support.

v1.4.4 (2018-06-30)
------

* Replication fixes: Cannot fully switch to replica mode until a valid decode position has been
  established, and fix calculation of commit position when replicating large updates. In both
  cases, an invalid log position could be used for recovery.
* More efficient loads for union, intersection, and difference views, when using the built-in
  combiners.
* Allow the Sorter.reset method to stop the sort when in the finishing step.
* Add sort methods which produce results directly into single-use scanners.
* Add a public Database.isClosed method.

v1.4.3.2 (2018-05-28)
--------
* Fix race conditions when skipping and counting tree entries as concurrent modifications are
  made. Subtrees might get skipped. https://github.com/cojen/Tupl/issues/102
* Fix potential memory leak when calling next and previous, introduced by the last fix. Cursors
  must be fully reset when reaching the end, now that frames are popped as late as possible.
* Fix potential stack overflow when iterating in reverse direction (previous) over a tree which
  is full of empty nodes. In practice this shouldn't have happened anyhow, since node merging
  is typically aggressive.

v1.4.3.1 (2018-05-19)
--------
* Fix race conditions in the cursor iteration methods, next and previous. Sometimes entire
  subtrees would get skipped over when another thread is making concurrent changes to the same
  region of the tree. https://github.com/cojen/Tupl/issues/101
* Sorter shouldn't split work with other threads which are currently being processed, since it
  interferes with duplicate detection. https://github.com/cojen/Tupl/issues/101

v1.4.3 (2018-04-14)
------
* Added maxCheckpointThreads option, which can speed up checkpoints.

v1.4.2 (2018-03-11)
------
* Fix temporary stall when calling the sorter progress method. The finish method didn't stop
  all the internal sorts as quickly as possible.
* Add feature to listen for when indexes are opened.
* Log events when running the verify and compact tools, and also allow the cache size to be set.

v1.4.1 (2018-02-02)
------
* Fix bug when using the new cursor registration feature. When the database is restarted,
  registered cursors could get lost, causing redo operations against them to be lost too.
* Honor the default durability mode when using a null transaction. Previously, when the default
  mode is NO_REDO and the transaction is null, a redo log entry would still be created. Also
  honor the transaction-specified durability mode when using the unsafe locking mode. Previously,
  the default durability mode would be used.
* Added support for two-phase commit, using the new transaction prepare and getId methods.
* Added a parallel external mergesort utility.

v1.4.0.2 (2018-01-21)
--------
* Fix bug when deleting entries from the largest allowed page size, 65536 bytes. Under the right
  conditions, the internal search vector pointer would overflow the 16-bit range.

v1.4.0.1 (2018-01-01)
--------
* Added a RAFT-based replication system.
* Added a ValueAccessor interface which supports very large values (>2Gib), random access,
  appending, truncation, zero-filling, and streams.
* Added Scanners and Updaters, which can be simpler and more efficient than using a Cursor.
* Added a key-only view, intended for use with scanners and updaters.
* Added various set views: union, intersection, and difference.
* Added support for event filtering and logging, with more events defined.
* Added comparator access to views and cursors.
* Added cursor registration, which can speed up replica performance when modifying value ranges.
* Added cursor findNearby methods for relational operators (GE, GT, LE, LT).
* Added a cursor close method, so that it can be used with try-with-resources statements.
* Added a view method to check for the existence of a value without loading it.
* Added a view method to conditionally update a value, based on equality.
* Added a view method to touch a key, as a convenient way to lock it.
* Added a transaction flush method, which helps ensure that replicated changes propagate early.
* Added a transaction lock combining method, as an alternative to nested scopes.
* Added a dirty pages stat counter, and include internal index stats.
* Added support for direct I/O (O_DIRECT).
* Added a CRC-32C utility (to bridge the gap with Java 9)
* Added more control over file preallocation when extending the length.
* Files are opened with the random access hint by default, eliminating unnecessary prefetching.
* Improved concurrency with the introduction of a "clutch", replacing ordinary latches.
* Incomplete database restoration is now detected, causing an IncompleteRestoreException to be
  thrown when opening the database.
* Unpositioned cursors now throw a specialized exception, still extending IllegalStateException.
* The index exchange/insert/replace/update operations are now always atomic, even when using
  the BOGUS transaction.

v1.3.12.3 (2017-05-20)
---------
* Fix race condition when capturing pages during a snapshot.
* Fix transaction id reset to zero when opening db.
* Fix race condition when creating an index with replication.
* Fix a rare deadlock when merging a tree node into a sibling node.
* Fix double delete of header page when closing database concurrently with a checkpoint.
* Eliminate potentially thread-unsafe code and improve handling of leader failover.
* Ensure that replication consumer thread exits when switching to replica mode.
* Prevent double database close.
* Rollback after failed file preallocation.
* Fix GC race condition in test.

v1.3.12.2 (2017-04-15)
---------
* Fix illegal latch release during recovery.
* Fix deadlock during replication subsystem checkpoint.
* Fix race condition when dropping an index
* Fix race conditions when accessing indexes while database is being closed.
* Refined behavior when storing nulls into default and constrained views.

v1.3.12.1 (2017-04-01)
---------
* Fix stack overflow error when skipping by zero on a transformed view.
* Copied cursor should have NOT_LOADED value assigned when applicable.
* Support redo tracing when replication manager is installed.
* Storing unsupported keys into transformed views shouldn't fail in all cases.
* Safely permit quick lock availability checks.

v1.3.12 (2017-03-18)
-------
* Fix recovery handling of fragmented and deleted entries.
* Ensure that broken transactions always attempt to write rollback operations into the redo log.
* Disable emptying of trash following failover. It races with concurrent transaction undo
  operations.
* Undo log fixes for fully mapped mode, for large transactions, for many open transactions, for
  large keys, and for large values.
* Fix various race conditions which can corrupt the node map data structure.
* Fix inline fragmented value calculation to consider that a remainder of zero means that no
  inline length field is required.
* Limit maximum key size based on the maximum entry size. With a page size of 4096, the maximum
  was 2026 bytes, but now it's 2024. Larger keys are fragment encoded as before.
* Fix redo writer latch state when switching redo writers.
* Index rename and delete operations should always commit against the same redo writer that the
  transaction uses.
* Fix stuck checkpoints following leader failover.
* Fix handling of commit operations which force the buffer to be flushed early.
* Fix handling of delete commit operation.
* Fix shared lock reentrancy bug when another thread is waiting in the queue.
* Fix ghost deletion race condition.
* Always rollback recovered transactions which shouldn't be replicated, unless they were
  explicitly committed.
* More latch performance tweaks.
* Improved performance of writing into the redo log when under heavy contention.
* Improved performance when accessing mapped files.
* Add open options for readahead and close hinting to increase page cache efficiency.
* Reduce contention on application threads when snapshots are in progress.
* Promptly unblock threads waiting on locks when database is closed.
* New threading model for ReplRedoEngine to support concurrent replication processing.

v1.3.11 (2017-01-28)
-------
* New latch implementation which offers higher performance on multi-core hardware and also uses
  less memory.
* Redo log decoding changes, to prevent compatibility issues with future versions.
* Writes to the redo log are now performed after applying any index changes. This allows the redo
  log to block without holding node latch, improving concurrency.
* Replication manager interface changes.
* Java 9 compatibility fixes.

v1.3.10.3 (2016-12-26)
---------
* Fix striped transaction id stride value.
* Fix variable length decoding of large 64-bit signed values.
* Don't attempt deleting root node of deleted tree after database has been closed.
* Use thread-local object when waiting for a lock, reducing garbage accumulation.

v1.3.10.2 (2016-12-10)
---------
* Fix deadlock when dropping an index.
* Fix maximum size calculation when fragmenting a value to fit into a split node. Entries with
  large keys would sometimes be rejected.

v1.3.10.1 (2016-12-05)
---------
* Fix regression bug created by earlier fix which allowed a split node to become empty.

v1.3.10 (2016-12-03)
-------
* Fix bug which allowed a split node to become empty.
* Optimized transactional deletes.

v1.3.9 (2016-11-27)
------
* Avoid looping indefinitely if random search encounters ghosts.
* Fix split insert handling of large keys and values which caused an overflow.
* Fix edge cases when storing large values into split nodes, and the values must be fragmented.
* Add method to reset a transaction due to an exception.
* Added a convenience method to create transactions from view instances.
* Cursor exceptions suppressed when database is closed.
* Optimize handling of shared commit lock reentrancy by eliminating a contended write.
* Optimize generation of random numbers, used internally.
* Update node MRU position less aggressively, improving performance due to fewer memory writes.

v1.3.8 (2016-10-29)
------
* Fix memory leaks when processing replicated transactions.
* Use a larger redo log buffer, to better cope with stalls when writing to the file.
* Stripe transaction state to improve concurrency.
* Don't run shutdown hooks when panicking, to avoid deadlocks.
* More replicated rollback improvements.

v1.3.7.1 (2016-10-22)
--------
* Fix when updating a large value into a newly split node.
* Fix race conditions when handling tree node stubs.
* Replicated transaction rollback should propagate immediately.

v1.3.7 (2016-10-15)
------
* Fix corruption caused by cursor traversal into split nodes.
* Fix corruption caused by broken cursor binding following a node split and merge.
* Fix latch upgrade race condition when deleting a ghost.
* Fix race conditions when handling parent-before-child node evictions.
* Fix race conditions when deleting and creating root node.
* Fix snapshot handling when using fully mapped mode.
* Fix stub node implementation when using fully mapped mode.
* Add support for attaching objects to transactions, for tracking them.
* Detect deadlocks when lock timeout is zero, for non-try variants.
* Suppress transaction exceptions from cleanup methods when database is closed.

v1.3.6 (2016-09-05)
------
* Fix updating of large entries into crammed nodes, which caused entries to get lost.
* Fix database lock file retention issue.
* Refine Cursor lock method to ensure that the latest value is retrieved.
* Fix key order check when running verification.
* Added file preallocation option. Allows early handling of disk full exceptions to prevent
  crashes with SIGBUS in the case where the file is mmap'ed and a delayed block allocation fails
  due to no space left on device.

v1.3.5 (2016-08-28)
------
* Fix NullPointerException when too many nodes are unevictable. A CacheExhaustedException
  should be thrown instead.
* Fix deadlock between node split and checkpoint.
* Fix "Already in NodeMap" exception when loading fragmented nodes.
* Fix for a rare assertion error when deleting the root node of a tree.
* Added a Cursor lock method, for manual lock control.
* Added some default View and Cursor method implementations.

v1.3.3.1 (2016-08-02)
--------
* Fix subtraction error when load encounters a split node, causing wrong value to be loaded.

v1.3.3 (2016-07-30)
------
* Fixed transaction race condition which allowed shared locks to be prematurely released.
* Fixed load race conditions which caused an incorrect value to be returned.
* Fix for performing database compaction while old indexes are concurrently deleted. Some
  pages would get lost, preventing compaction from ever working again.
* Support temporary indexes.
* Don't close in-use indexes during verification.
* Redo decoder should be lenient if EOF is reached in the middle of an operation.
* Rewrite CommitLock to stripe shared lock requests, improving concurrency.
* Use Java 9 vectorized comparison method if available.
* Add full stats support for non-durable databases.

v1.3.2 (2016-06-04)
------
* Fix storage leak when database capacity is reached during fragmented value allocation.
* Fix deadlock when gathering stats while trees are concurrently closed.
* Optimize count method, utilizing stored internal node counts.
* Add file I/O support for ByteBuffers.

v1.3.1 (2016-05-07)
------
* Fix handling of invalidated transactions, and defined a new exception type for it.
* Fix root node initialization for new trees when using fully mapped mode.
* Fix various replication issues.
* Fix race conditions when closing database.
* Prevent improper use of bogus transaction.

v1.3.0.1 (2016-04-16)
--------
* Fix for Index.load during concurrent node splits. It caused the load to falsely return null.
* Fix undo log node creation when using direct page access mode. The reserved byte was not
  explicitly cleared, allowing fragmented values to corrupt the nodes as pages get recycled.
* Fix shared latch double release when using VARIANT_RETAIN, which would result in a
  deadlock. This affected the Cursor.findGe and Cursor.findLe methods.
* Fix handling of Index.evict when encountering empty nodes, and lock keys as required by the
  transaction.
* Fix handling of index delete and recovery. Deleted index must be closed just like they were
  before recovery, to allow recovery to complete.
* Fix for deleting empty indexes which caused an exception.
* Eliminate overhead of zero-length memory copy when using direct page access mode. This
  primarily affected the performance of values larger than the page size.
* Allow node merge to propagate upwards for empty nodes.
* Ensure that compaction and verification visit all nodes, even empty ones.

v1.3.0 (2016-04-02)
------
* Depends on Java 8.
* Several top-level classes are now interfaces.
* Tree search operations rely extensively on shared latches instead of exclusive latches,
  improving concurrency.
* Fix cursor race condition which allowed split nodes to be modified too soon, leading to
  database corruption.
* Fix deadlock when closing database.
* Fix handling of mapped file shrinkage on Windows.
* More fixes for random search and add improve safety of frame binding.
* Added method to analyze index size.
* Added capacity limit feature.
* Added fully mapped mode when using direct page access and MappedPageArray.
* Added method to evict records from an Index.
* File sync improvements for Linux and MacOS. Performs directory sync'ng and F_FULLSYNC.
* Use JNA to access native I/O functions, eliminating extra system calls.
* Make Latch class a public utility.

v1.2.7.1 (2015-12-22)
--------
* Fix defect in cursor skip which might operate against an unlatched node.

v1.2.7 (2015-10-04)
------
* Fixed reverse view range handling.
* Fixed compareKeyTo methods for transformed and trimmed views.
* Require that bounded views only operate on ordered views.
* Added method to count entries in a View.
* Exposed a few more utility methods.
* When using direct page access mode, page fields are no longer copied to Node instance
  fields. This reduces overall Java heap memory footprint.

v1.2.6.1 (2015-09-05)
--------
* Fix when using mapped files on Linux. Shrinking the database file would cause the process to
  crash when the file is accessed again.

v1.2.6 (2015-08-30)
------
* Counts stored in bottom internal nodes, for speeding up cursor skip operations.
* Added skip method which accepts a limit key.
* More failure handling improvements.

v1.2.5 (2015-07-19)
------
* Fix non-transactional delete race condition.
* Fixes for random cursor search, when encountering internal nodes with few entries or when
  the search range is empty.
* Bug fix for findNearby acting on a closed index.
* Improvements for handling temporary write failures.
* Allow replicas to create no-redo transactions.
* Added option to change transaction durability mode.
* Define an evict operation on the page array, for eliminating unnecessary copies.
* Introduce concurrent cache priming.

v1.2.4 (2015-06-14)
------
* Fix node delete race condition which triggered an assertion error.
* Fix case in which a cursor value was not set if it caused a node merge.
* Add basic AbstractCursor implementation.
* Merge custom redo and undo handler interfaces into one.
* Add checkpoint support to custom transaction handler.
* Provide access to transaction nesting level.
* Attempting to write into an unmodifiable transaction should not invalidate it.
* Introduce combined store and commit operation.

v1.2.3 (2015-05-16)
------
* Fix snapshot deadlock when reading from the cache.
* Add support for custom transaction operations.
* Improved key hash function used by lock manager.
* Expose the WrappedCursor class.
* Added experimental direct page implementation which relies on Unsafe features. Modify
  the source using PageAccessTransformer and then recompile.

v1.2.2 (2015-04-12)
------
* Fix root node deletion when deleting an index.
* Fix memory leak when closing an index.
* Fix to ensure that undo log recovery tracks ghosted values.
* Index drop uses same code as index delete, eliminating complex duplicate code and
  inconsistent behavior.
* Minor thread-safety fix when performing database compaction.
* Created a package for low-level extensions.

v1.2.1 (2015-03-21)
------
* Prevent search deadlock caused by heavy eviction.
* Handle rare NPE when a non-root node becomes the root node during a split.
* Snapshot can read from the cache.
* Allow checkpoint when doing page preallocation.

v1.2.0 (2015-03-01)
------
* Support large keys (up to 2GiB).
* Increased small key encoding format from 64 bytes to 128 bytes.

v1.1.12 (2015-03-01)
------
* Ensure that recovery deletes ghosts of large deleted values.
* Added support for filtering and transforming.
* Added direct lock control to views.
* Support large keys (up to 2GiB) as an experimental option.

v1.1.11 (2015-02-08)
------
* Ensure that any failure to push into the undo log is cleaned up.
* Modify replication interface to handle race condition during checkpoint.
* Ensure durability of index creation and alteration operations.
* Enhancements for safely handling checkpoint abort.
* Support weaker durability modes with replication.

v1.1.10 (2015-01-04)
------
* Fix race conditions in node allocation and recycling code.
* Minor performance optimizations for large values and for simple loads.

v1.1.9 (2014-12-15)
------
* Fix assertion failure caused by aggressive eviction before the very first checkpoint.
* Added simple verification and file compaction tools.
* Checkpoint and redo durability improvements.

v1.1.8 (2014-11-30)
------
* Fix for corruption caused by parent nodes being evicted before their child nodes.
* Stripe page allocation to improve concurrency.
* Merge extremity nodes less aggressively, optimizing for queue access patterns.

v1.1.7 (2014-10-19)
------
* Fix for temporary deadlock at the beginning of a checkpoint.

v1.1.6 (2014-10-11)
------
* Added support for a secondary cache. A secondary cache is slower than a primary cache, but a
  very large primary cache can cause high garbage collection overhead.
* Added method to delete a non-empty index.
* Closing an index no longer forces all the tree nodes to be evicted.

v1.1.5 (2014-09-07)
------
* New node cache implementation. It reduces memory overhead, garbage collection activity, and
  performs fewer memory copies when inserting records.

v1.1.4 (2014-07-13)
------
* Fix corruption when reopening database, caused by aggressive page recycling.

v1.1.3 (2014-06-22)
------
* Reduce the number of node compactions caused by leaf rebalancing, by requiring that an
  existing entry slot be re-used for the inserted entry. More aggressive rebalancing is
  possible, but it causes even more compaction overhead with little benefit.

v1.1.2 (2014-05-29)
------
* Fix rare node eviction bug which left it (partially) in the usage list.
* Cache growth is no longer aggressive, only doing so to avoid node eviction.
* Added cache size to stats.
* Defined new DatabaseFullException for non-durable databases.

v1.1.1 (2014-05-12)
------

* Target Java 7 only.
* Added support for cache priming.
* Various fixes.

v1.1.0 (2014-02-15)
------

* Improved performance of findNearby by encoding extra b-tree node metadata. Indexes can be
  rapidly filled with in-order records using a single cursor which advances to the next key
  using findNearby. This technique works with reverse ordered records and it allows concurrent
  access as usual. Indexes created in older versions will not have this metadata, and so they
  must be rebuilt to obtain it.
* Decode only 6-byte child pointers, preparing for future support of child entry counts.

v1.0.5 (2014-02-01)
------

* Reveal file compaction feature.
* Tolerate, but don't update, b-tree node metadata introduced in version 1.1.0.

v1.0.4 (2014-01-19)
------

* Fix defects when inserting largest possible keys.
* Added rebalancing for internal nodes (was only leaf nodes before).
* Added key suffix compression, which allows more keys to fit inside internal nodes.
* Enforce key size limits. 

v1.0.3 (2014-01-11)
------

* Fix root node corruption when inserting large keys.
* Allow database with non-default page size to be opened without requiring page size to be
  explicitly configured.
* Remove unimplemented node rebalancing for deletes and remove unnecessary node dirtying.
* Added experimental file compaction feature.

v1.0.2 (2013-12-22)
------

* Prevent corruption of recycle free list due to high recycle rate and checkpoints.
* Add equals and hashCode methods to Database.Stats object.
* Restrict page size to be even. Doesn't break compatibility because odd sized pages would lead
  to database corruption anyhow. Supporting odd sized pages isn't worth the effort.

v1.0.1 (2013-12-01)
------

* Index close allows active cursors now. The cursors will behave the same as if they were
  created after the index was closed. The index will appear to be empty and all modifications
  throw a ClosedIndexException.
* Added ability to rename an index.
* Added index rename and drop notifications.
* Database shutdown method works for non-durable databases.
* Added node rebalancing to reduce splits and reduce storage overhead.

v1.0.0 (2013-10-07)
------

* First released version.
