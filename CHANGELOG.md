Changelog
=========

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
