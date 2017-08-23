/*
 *  Copyright (C) 2011-2017 Cojen.org
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl;

import java.io.Flushable;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

import java.nio.charset.StandardCharsets;

import org.cojen.tupl.io.CauseCloseable;

import static org.cojen.tupl.Utils.*;

/**
 * Primary database interface, containing a collection of transactional indexes. Call {@link
 * #open open} to obtain a Database instance. Examples:
 *
 * <p>Open a non-durable database, limited to a max size of 100MB:
 *
 * <pre>
 * DatabaseConfig config = new DatabaseConfig().maxCacheSize(100_000_000);
 * Database db = Database.open(config);
 * </pre>
 *
 * <p>Open a regular database, setting the minimum cache size to ensure enough
 * memory is initially available. A weak {@link DurabilityMode durability mode}
 * offers the best transactional commit performance.
 *
 * <pre>
 * DatabaseConfig config = new DatabaseConfig()
 *    .baseFilePath("/var/lib/tupl/myapp")
 *    .minCacheSize(100_000_000)
 *    .durabilityMode(DurabilityMode.NO_FLUSH);
 *
 * Database db = Database.open(config);
 * </pre>
 *
 * <p>The following files are created by the above example:
 *
 * <ul>
 * <li><code>/var/lib/tupl/myapp.db</code> &ndash; primary data file
 * <li><code>/var/lib/tupl/myapp.info</code> &ndash; text file describing the database configuration
 * <li><code>/var/lib/tupl/myapp.lock</code> &ndash; lock file to ensure that at most one process can have the database open
 * <li><code>/var/lib/tupl/myapp.redo.0</code> &ndash; first transaction redo log file
 * </ul>
 *
 * <p>New redo log files are created by {@link #checkpoint checkpoints}, which
 * also delete the old files. When {@link #beginSnapshot snapshots} are in
 * progress, one or more numbered temporary files are created. For example:
 * <code>/var/lib/tupl/myapp.temp.123</code>.
 *
 * @author Brian S O'Neill
 * @see DatabaseConfig
 */
public interface Database extends CauseCloseable, Flushable {
    /**
     * Open a database, creating it if necessary.
     */
    public static Database open(DatabaseConfig config) throws IOException {
        return config.open(false, null);
    }

    /**
     * Delete the contents of an existing database, and replace it with an
     * empty one. When using a raw block device for the data file, this method
     * must be used to format it.
     */
    public static Database destroy(DatabaseConfig config) throws IOException {
        return config.open(true, null);
    }

    /**
     * Returns the given named index, returning null if not found.
     *
     * @return shared Index instance; null if not found
     */
    public abstract Index findIndex(byte[] name) throws IOException;

    /**
     * Returns the given named index, returning null if not found. Name is UTF-8
     * encoded.
     *
     * @return shared Index instance; null if not found
     */
    public default Index findIndex(String name) throws IOException {
        return findIndex(name.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Returns the given named index, creating it if necessary.
     *
     * @return shared Index instance
     */
    public abstract Index openIndex(byte[] name) throws IOException;

    /**
     * Returns the given named index, creating it if necessary. Name is UTF-8
     * encoded.
     *
     * @return shared Index instance
     */
    public default Index openIndex(String name) throws IOException {
        return openIndex(name.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Returns an index by its identifier, returning null if not found.
     *
     * @throws IllegalArgumentException if id is reserved
     */
    public abstract Index indexById(long id) throws IOException;

    /**
     * Returns an index by its identifier, returning null if not found.
     *
     * @param id big-endian encoded long integer
     * @throws IllegalArgumentException if id is malformed or reserved
     */
    public default Index indexById(byte[] id) throws IOException {
        if (id.length != 8) {
            throw new IllegalArgumentException("Expected an 8 byte identifier: " + id.length);
        }
        return indexById(decodeLongBE(id, 0));
    }

    /**
     * Renames the given index to the one given.
     *
     * @param index non-null open index
     * @param newName new non-null name
     * @throws ClosedIndexException if index reference is closed
     * @throws IllegalStateException if name is already in use by another index
     * @throws IllegalArgumentException if index belongs to another database instance
     */
    public abstract void renameIndex(Index index, byte[] newName) throws IOException;

    /**
     * Renames the given index to the one given. Name is UTF-8 encoded.
     *
     * @param index non-null open index
     * @param newName new non-null name
     * @throws ClosedIndexException if index reference is closed
     * @throws IllegalStateException if name is already in use by another index
     * @throws IllegalArgumentException if index belongs to another database instance
     */
    public default void renameIndex(Index index, String newName) throws IOException {
        renameIndex(index, newName.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Fully closes and deletes the given index, but does not immediately reclaim the pages it
     * occupied. Run the returned task in any thread to reclaim the pages.
     *
     * <p>Once deleted, the index reference appears empty and {@link ClosedIndexException
     * unmodifiable}. A new index by the original name can be created, which will be assigned a
     * different unique identifier. Any transactions still referring to the old index will not
     * affect the new index.
     *
     * <p>If the deletion task is never started or it doesn't finish normally, it will resume
     * when the database is re-opened. All resumed deletions are completed in serial order by a
     * background thread.
     *
     * @param index non-null open index
     * @return non-null task to call for reclaiming the pages used by the deleted index
     * @throws ClosedIndexException if index reference is closed
     * @throws IllegalArgumentException if index belongs to another database instance
     * @see EventListener
     * @see Index#drop Index.drop
     */
    public abstract Runnable deleteIndex(Index index) throws IOException;

    /**
     * Creates a new unnamed temporary index. Temporary indexes never get written to the redo
     * log, and they are deleted when the database is re-opened.
     */
    public abstract Index newTemporaryIndex() throws IOException;

    /**
     * Returns an {@link UnmodifiableViewException unmodifiable} View which maps all available
     * index names to identifiers. Identifiers are long integers, {@link
     * org.cojen.tupl.io.Utils#decodeLongBE big-endian} encoded.
     */
    public abstract View indexRegistryByName() throws IOException;

    /**
     * Returns an {@link UnmodifiableViewException unmodifiable} View which maps all available
     * index identifiers to names. Identifiers are long integers, {@link
     * org.cojen.tupl.io.Utils#decodeLongBE big-endian} encoded.
     */
    public abstract View indexRegistryById() throws IOException;

    /**
     * Returns a new Transaction with the {@link DatabaseConfig#durabilityMode default}
     * durability mode.
     */
    public default Transaction newTransaction() {
        return newTransaction(null);
    }

    /**
     * Returns a new Transaction with the given durability mode. If null, the
     * {@link DatabaseConfig#durabilityMode default} is used.
     */
    public abstract Transaction newTransaction(DurabilityMode durabilityMode);

    /**
     * Preallocates pages for immediate use. The actual amount allocated
     * varies, depending on the amount of free pages already available.
     *
     * @return actual amount allocated
     */
    public abstract long preallocate(long bytes) throws IOException;

    /**
     * Set a soft capacity limit for the database, to prevent filling up the storage
     * device. When the limit is reached, writes might fail with a {@link
     * DatabaseFullException}. No explicit limit is defined by default, and the option is
     * ignored by non-durable databases. The limit is checked only when the database attempts
     * to grow, and so it can be set smaller than the current database size.
     *
     * <p>Even with a capacity limit, the database file can still slowly grow in size.
     * Explicit overrides and critical operations can allocate space which can only be
     * reclaimed by {@link #compactFile compaction}.
     *
     * @param bytes maximum capacity, in bytes; pass -1 for no limit
     */
    public default void capacityLimit(long bytes) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the current capacity limit, rounded down by page size.
     *
     * @return maximum capacity, in bytes; is -1 if no limit
     */
    public default long capacityLimit() {
        return -1;
    }

    /**
     * Set capacity limits for the current thread, allowing it to perform tasks which can free
     * up space. While doing so, it might require additional temporary storage.
     *
     * @param bytes maximum capacity, in bytes; pass -1 for no limit; pass 0 to remove override
     */
    public default void capacityLimitOverride(long bytes) {
        throw new UnsupportedOperationException();
    }

    /**
     * Support for capturing a snapshot (hot backup) of the database, while
     * still allowing concurrent modifications. The snapshot contains all data
     * up to the last checkpoint. Call the {@link #checkpoint checkpoint}
     * method immediately before to ensure that an up-to-date snapshot is
     * captured.
     *
     * <p>To restore from a snapshot, store it in the primary data file, which
     * is the base file with a ".db" extension. Make sure no redo log files
     * exist and then open the database. Alternatively, call {@link
     * #restoreFromSnapshot restoreFromSnapshot}, which also supports restoring
     * into separate data files.
     *
     * <p>During the snapshot, temporary files are created to hold pre-modified
     * copies of pages. If the snapshot destination stream blocks for too long,
     * these files keep growing. File growth rate increases too if the database
     * is being heavily modified. In the worst case, the temporary files can
     * become larger than the primary database files.
     *
     * @return a snapshot control object, which must be closed when no longer needed
     */
    public abstract Snapshot beginSnapshot() throws IOException;

    /**
     * Restore from a {@link #beginSnapshot snapshot}, into the data files defined by the given
     * configuration. All existing data and redo log files at the snapshot destination are
     * deleted before the restore begins.
     *
     * @param in snapshot source; does not require extra buffering; auto-closed
     */
    public static Database restoreFromSnapshot(DatabaseConfig config, InputStream in)
        throws IOException
    {
        return config.open(false, in);
    }

    /**
     * Writes a cache priming set into the given stream, which can then be used later to {@link
     * #applyCachePrimer prime} the cache.
     *
     * @param out cache priming destination; buffering is recommended; not auto-closed
     * @see DatabaseConfig#cachePriming
     */
    public abstract void createCachePrimer(OutputStream out) throws IOException;

    /**
     * Prime the cache, from a set encoded {@link #createCachePrimer earlier}.
     *
     * @param in caching priming source; buffering is recommended; not auto-closed
     * @see DatabaseConfig#cachePriming
     */
    public abstract void applyCachePrimer(InputStream in) throws IOException;

    /**
     * Returns a collection of database statistics.
     */
    public abstract Stats stats();

    /**
     * Collection of database {@link Database#stats statistics}.
     */
    public static class Stats implements Cloneable, Serializable {
        private static final long serialVersionUID = 3L;

        public int pageSize;
        public long freePages;
        public long totalPages;
        public long cachedPages;
        public long dirtyPages;
        public int openIndexes;
        public long lockCount;
        public long cursorCount;
        public long txnCount;
        public long txnsCreated;

        /**
         * Returns the allocation page size.
         */
        public int pageSize() {
            return pageSize;
        }

        /**
         * Returns the amount of unused pages in the database.
         */
        public long freePages() {
            return freePages;
        }

        /**
         * Returns the total amount of pages in the database.
         */
        public long totalPages() {
            return totalPages;
        }

        /**
         * Returns the current size of the cache, in pages.
         */
        public long cachedPages() {
            return cachedPages;
        }

        /**
         * Returns the count of pages which are dirty (need to be written with a checkpoint).
         */
        public long dirtyPages() {
            return dirtyPages;
        }

        /**
         * Returns the amount of indexes currently open.
         */
        public int openIndexes() {
            return openIndexes;
        }

        /**
         * Returns the amount of locks currently allocated. Locks are created as transactions
         * access or modify records, and they are destroyed when transactions exit or reset. An
         * accumulation of locks can indicate that transactions are not being reset properly.
         */
        public long lockCount() {
            return lockCount;
        }

        /**
         * Returns the amount of cursors which are in a non-reset state. An accumulation of
         * cursors can indicate that they are not being reset properly.
         */
        public long cursorCount() {
            return cursorCount;
        }

        /**
         * Returns the amount of fully-established transactions which are in a non-reset
         * state. This value is unaffected by transactions which make no changes, and it is
         * also unaffected by auto-commit transactions. An accumulation of transactions can
         * indicate that they are not being reset properly.
         */
        public long transactionCount() {
            return txnCount;
        }

        /**
         * Returns the total amount of fully-established transactions created over the life of
         * the database. This value is unaffected by transactions which make no changes, and it
         * is also unaffected by auto-commit transactions. A resurrected transaction can become
         * fully-established again, further increasing the total created value.
         */
        public long transactionsCreated() {
            return txnsCreated;
        }

        @Override
        public Stats clone() {
            try {
                return (Stats) super.clone();
            } catch (CloneNotSupportedException e) {
                throw Utils.rethrow(e);
            }
        }

        @Override
        public int hashCode() {
            long hash = freePages;
            hash = hash * 31 + totalPages;
            hash = hash * 31 + txnsCreated;
            return (int) scramble(hash);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj != null && obj.getClass() == Stats.class) {
                Stats other = (Stats) obj;
                return pageSize == other.pageSize
                    && freePages == other.freePages
                    && totalPages == other.totalPages
                    && cachedPages == other.cachedPages
                    && dirtyPages == other.dirtyPages
                    && openIndexes == other.openIndexes
                    && lockCount == other.lockCount
                    && cursorCount == other.cursorCount
                    && txnCount == other.txnCount
                    && txnsCreated == other.txnsCreated;
            }
            return false;
        }

        @Override
        public String toString() {
            return "Database.Stats {pageSize=" + pageSize
                + ", freePages=" + freePages
                + ", totalPages=" + totalPages
                + ", cachedPages=" + cachedPages
                + ", dirtyPages=" + dirtyPages
                + ", openIndexes=" + openIndexes
                + ", lockCount=" + lockCount
                + ", cursorCount=" + cursorCount
                + ", transactionCount=" + txnCount
                + ", transactionsCreated=" + txnsCreated
                + '}';
        }
    }

    /**
     * Flushes all committed transactions, but not durably. Transactions committed with {@link
     * DurabilityMode#NO_FLUSH no-flush} effectively become {@link DurabilityMode#NO_SYNC
     * no-sync} durable.
     */
    @Override
    public abstract void flush() throws IOException;

    /**
     * Durably flushes all committed transactions. Transactions committed with {@link
     * DurabilityMode#NO_FLUSH no-flush} and {@link DurabilityMode#NO_SYNC no-sync} effectively
     * become {@link DurabilityMode#SYNC sync} durable.
     */
    public abstract void sync() throws IOException;

    /**
     * Durably sync and checkpoint all changes to the database. In addition to ensuring that
     * all committed transactions are durable, checkpointing ensures that non-transactional
     * modifications are durable. Checkpoints are performed automatically by a background
     * thread, at a {@link DatabaseConfig#checkpointRate configurable} rate.
     */
    public abstract void checkpoint() throws IOException;

    /**
     * Temporarily suspend automatic checkpoints without waiting for any in-progress checkpoint
     * to complete. Suspend may be invoked multiple times, but each must be paired with a
     * {@link #resumeCheckpoints resume} call to enable automatic checkpoints again.
     *
     * @throws IllegalStateException if suspended more than 2<sup>31</sup> times
     */
    public abstract void suspendCheckpoints();

    /**
     * Resume automatic checkpoints after having been temporarily {@link #suspendCheckpoints
     * suspended}.
     *
     * @throws IllegalStateException if resumed more than suspended
     */
    public abstract void resumeCheckpoints();

    /**
     * Compacts the database by shrinking the database file. The compaction target is the
     * desired file utilization, and it controls how much compaction should be performed. A
     * target of 0.0 performs no compaction, and a value of 1.0 attempts to compact as much as
     * possible.
     *
     * <p>If the compaction target cannot be met, the entire operation aborts. If the database
     * is being concurrently modified, large compaction targets will likely never succeed.
     * Although compacting by smaller amounts is more likely to succeed, the entire database
     * must still be scanned. A minimum target of 0.5 is recommended for the compaction to be
     * worth the effort.
     *
     * <p>Compaction requires some amount of free space for page movement, and so some free
     * space might still linger following a massive compaction. More iterations are required to
     * fully complete such a compaction. The first iteration might actually cause the file to
     * grow slightly. This can be prevented by doing a less massive compaction first.
     *
     * @param observer optional observer; pass null for default
     * @param target database file compaction target [0.0, 1.0]
     * @return false if file compaction aborted
     * @throws IllegalArgumentException if compaction target is out of bounds
     * @throws IllegalStateException if compaction is already in progress
     */
    public abstract boolean compactFile(CompactionObserver observer, double target)
        throws IOException;

    /**
     * Verifies the integrity of the database and all indexes.
     *
     * @param observer optional observer; pass null for default
     * @return true if verification passed
     */
    public abstract boolean verify(VerificationObserver observer) throws IOException;

    /**
     * Closes the database, ensuring durability of committed transactions. No
     * checkpoint is performed by this method, and so non-transactional
     * modifications can be lost.
     *
     * @see #shutdown
     */
    @Override
    public default void close() throws IOException {
        close(null);
    }

    /**
     * Closes the database after an unexpected failure. No checkpoint is performed by this
     * method, and so non-transactional modifications can be lost.
     *
     * @param cause if non-null, delivers a {@link EventType#PANIC_UNHANDLED_EXCEPTION panic}
     * event and future database accesses will rethrow the cause
     * @see #shutdown
     */
    @Override
    public abstract void close(Throwable cause) throws IOException;

    /**
     * Cleanly closes the database, ensuring durability of all modifications. A checkpoint is
     * issued first, and so a quick recovery is performed when the database is re-opened. As a
     * side effect of shutting down, all extraneous files are deleted.
     */
    public abstract void shutdown() throws IOException;
}
