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

import java.util.concurrent.locks.Lock;

import java.net.SocketAddress;

import java.nio.charset.StandardCharsets;

import javax.net.ssl.SSLContext;

import org.cojen.tupl.core.Rebuilder;

import org.cojen.tupl.diag.CompactionObserver;
import org.cojen.tupl.diag.DatabaseStats;
import org.cojen.tupl.diag.EventListener;
import org.cojen.tupl.diag.EventType;
import org.cojen.tupl.diag.VerificationObserver;

import org.cojen.tupl.ext.CustomHandler;
import org.cojen.tupl.ext.PrepareHandler;

import org.cojen.tupl.io.CauseCloseable;

import org.cojen.tupl.remote.ClientDatabase;

import org.cojen.tupl.table.join.JoinTableMaker;

import static org.cojen.tupl.core.Utils.*;

/**
 * Primary database interface, containing a collection of transactional indexes. Call {@link
 * #open open} to obtain a Database instance. Examples:
 *
 * <p>Open a non-durable database, limited to a max size of 100MB:
 *
 * {@snippet lang="java" :
 * var config = new DatabaseConfig().maxCacheSize(100_000_000);
 * Database db = Database.open(config);
 * Index data = db.openIndex("mydata");
 * }
 *
 * <p>Open a regular database, with a fixed cache size, and a weak {@linkplain DurabilityMode
 * durability mode} for the best transactional commit performance.
 *
 * {@snippet lang="java" :
 * var config = new DatabaseConfig()
 *    .baseFilePath("/var/lib/tupl/myapp")
 *    .cacheSize(100_000_000)
 *    .durabilityMode(DurabilityMode.NO_FLUSH);
 *
 * Database db = Database.open(config);
 * Index data = db.openIndex("mydata");
 * }
 *
 * <p>The following files are created by the above example:
 *
 * <ul>
 * <li><code>/var/lib/tupl/myapp.db</code> &ndash; primary database file
 * <li><code>/var/lib/tupl/myapp.lock</code> &ndash; lock file to ensure that at most one process can have the database open
 * <li><code>/var/lib/tupl/myapp.redo.0</code> &ndash; first transaction redo log file
 * </ul>
 *
 * <p>New redo log files are created by {@linkplain #checkpoint checkpoints}, which
 * also delete the old files. When {@linkplain #beginSnapshot snapshots} are in
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
        return config.mLauncher.open(false, null);
    }

    /**
     * Delete the contents of an existing database, and replace it with an empty one. When
     * using a raw block device for the data file, this method must be used to format it. When
     * database is replicated, calling destroy only affects the local replica.
     */
    public static Database destroy(DatabaseConfig config) throws IOException {
        return config.mLauncher.open(true, null);
    }

    /**
     * Establish a remote connection to a database which is running a {@link #newServer server}.
     *
     * @param context optionally pass a context to open a secure connection
     * @throws IllegalArgumentException if not given one or two tokens
     */
    public static Database connect(SocketAddress addr, SSLContext context, long... tokens)
        throws IOException
    {
        return ClientDatabase.connect(addr, context, tokens);
    }

    /**
     * Open an existing database and copy all the data into a new database, which can have a
     * different configuration.
     *
     * @param numThreads pass 0 for default, or if negative, the actual number will be {@code
     * (-numThreads * availableProcessors)}.
     * @return the newly built database
     * @throws IllegalStateException if the new database already exists or if either is replicated
     */
    public static Database rebuild(DatabaseConfig oldConfig, DatabaseConfig newConfig,
                                   int numThreads)
        throws IOException
    {
        if (numThreads <= 0) {
            int procs = Runtime.getRuntime().availableProcessors();
            numThreads = numThreads == 0 ? procs : procs * -numThreads;
        }
        return new Rebuilder(oldConfig.mLauncher, newConfig.mLauncher, numThreads).run();
    }

    /**
     * Returns the given named index, creating it if necessary.
     *
     * @return shared Index instance
     */
    public Index openIndex(byte[] name) throws IOException;

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
     * Returns the given named index, returning null if not found.
     *
     * @return shared Index instance; null if not found
     */
    public Index findIndex(byte[] name) throws IOException;

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
     * Returns an index by its identifier, returning null if not found.
     *
     * @return shared Index instance
     * @throws IllegalArgumentException if id is reserved
     */
    public Index indexById(long id) throws IOException;

    /**
     * Returns an index by its identifier, returning null if not found.
     *
     * @param id big-endian encoded long integer
     * @return shared Index instance
     * @throws IllegalArgumentException if id is malformed or reserved
     */
    public default Index indexById(byte[] id) throws IOException {
        if (id.length != 8) {
            throw new IllegalArgumentException("Expected an 8 byte identifier: " + id.length);
        }
        return indexById(decodeLongBE(id, 0));
    }

    /**
     * Convenience method which returns a {@code Table} that uses the index named by the row
     * type itself.
     *
     * @return shared {@code Table} instance
     * @see Index#asTable
     */
    public default <R> Table<R> openTable(Class<R> type) throws IOException {
        return openIndex(type.getName()).asTable(type);
    }

    /**
     * Convenience method which returns a {@code Table} that uses the index named by the row
     * type itself.
     *
     * @return shared {@code Table} instance; null if not found
     * @see Index#asTable
     */
    public default <R> Table<R> findTable(Class<R> type) throws IOException {
        Index ix = findIndex(type.getName());
        return ix == null ? null : ix.asTable(type);
    }

    /**
     * Convenience method which joins tables together, opening them if necessary.
     *
     * @param spec join specification
     * @throws NullPointerException if any parameters are null
     * @throws IllegalArgumentException if join type or specification is malformed
     * @see Table#join
     */
    public default <J> Table<J> openJoinTable(Class<J> joinType, String spec) throws IOException {
        return JoinTableMaker.join(joinType, spec, this);
    }

    /**
     * Renames the given index to the one given.
     *
     * @param index non-null open index
     * @param newName new non-null name
     * @throws ClosedIndexException if index reference is closed
     * @throws IllegalStateException if name is already in use by another index
     * @throws IllegalStateException if index belongs to another database instance
     */
    public void renameIndex(Index index, byte[] newName) throws IOException;

    /**
     * Renames the given index to the one given. Name is UTF-8 encoded.
     *
     * @param index non-null open index
     * @param newName new non-null name
     * @throws ClosedIndexException if index reference is closed
     * @throws IllegalStateException if name is already in use by another index
     * @throws IllegalStateException if index belongs to another database instance
     */
    public default void renameIndex(Index index, String newName) throws IOException {
        renameIndex(index, newName.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Fully closes and deletes the given index, but does not immediately reclaim the pages it
     * occupied. Run the returned task in any thread to reclaim the pages.
     *
     * <p>Once deleted, accesing the index causes {@linkplain DeletedIndexException exceptions}
     * to be thrown. A new index by the original name can be created, which will be assigned a
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
     * @throws IllegalStateException if index belongs to another database instance
     * @see EventListener
     * @see Index#drop Index.drop
     */
    public Runnable deleteIndex(Index index) throws IOException;

    /**
     * Creates a new unnamed temporary index. Temporary indexes never get written to the redo
     * log, and they are deleted when the database is re-opened. Temporary indexes should be
     * explicitly {@linkplain #deleteIndex deleted} when no longer needed, rather than waiting
     * until the database is re-opened.
     */
    public Index newTemporaryIndex() throws IOException;

    /**
     * Returns an {@linkplain UnmodifiableViewException unmodifiable} View which maps all
     * available index names to identifiers. Identifiers are long integers, {@linkplain
     * org.cojen.tupl.io.Utils#decodeLongBE big-endian} encoded.
     */
    public View indexRegistryByName() throws IOException;

    /**
     * Returns an {@linkplain UnmodifiableViewException unmodifiable} View which maps all
     * available index identifiers to names. Identifiers are long integers, {@linkplain
     * org.cojen.tupl.io.Utils#decodeLongBE big-endian} encoded.
     */
    public View indexRegistryById() throws IOException;

    /**
     * Returns a new Transaction with the {@linkplain DatabaseConfig#durabilityMode default}
     * durability mode.
     */
    public default Transaction newTransaction() {
        return newTransaction(null);
    }

    /**
     * Returns a new Transaction with the given durability mode. If null, the
     * {@linkplain DatabaseConfig#durabilityMode default} is used.
     */
    public Transaction newTransaction(DurabilityMode durabilityMode);

    /**
     * Returns a handler instance suitable for writing custom redo and undo operations. A
     * corresponding recovery instance must have been provided when the database was opened,
     * via the {@link DatabaseConfig#customHandlers customHandlers} config method.
     *
     * @return new writer instance
     * @throws IllegalStateException if no recovery instance by the given name is installed
     */
    public CustomHandler customWriter(String name) throws IOException;

    /**
     * Returns a handler instance suitable for preparing transactions. A corresponding recovery
     * instance must have been provided when the database was opened, via the {@link
     * DatabaseConfig#prepareHandlers prepareHandlers} config method.
     *
     * @return new writer instance
     * @throws IllegalStateException if no recovery instance by the given name is installed
     */
    public PrepareHandler prepareWriter(String name) throws IOException;

    /**
     * Returns a new Sorter instance. The standard algorithm is a parallel external mergesort,
     * which attempts to use all available processors. All external storage is maintained in
     * the database itself, in the form of temporary indexes.
     */
    public Sorter newSorter();

    /**
     * Preallocates pages for immediate use. The actual amount allocated
     * varies, depending on the amount of free pages already available.
     *
     * @return actual amount allocated
     */
    public long preallocate(long bytes) throws IOException;

    /**
     * Set a soft capacity limit for the database, to prevent filling up the storage
     * device. When the limit is reached, writes might fail with a {@link
     * DatabaseFullException}. No explicit limit is defined by default, and the option is
     * ignored by non-durable databases. The limit is checked only when the database attempts
     * to grow, and so it can be set smaller than the current database size.
     *
     * <p>Even with a capacity limit, the database file can still slowly grow in size.
     * Explicit overrides and critical operations can allocate space which can only be
     * reclaimed by {@linkplain #compactFile compaction}.
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
    public Snapshot beginSnapshot() throws IOException;

    /**
     * Restore from a {@linkplain #beginSnapshot snapshot}, into the data files defined by the
     * given configuration. All existing data and redo log files at the snapshot destination
     * are deleted before the restore begins.
     *
     * @param in snapshot source; does not require extra buffering; auto-closed
     */
    public static Database restoreFromSnapshot(DatabaseConfig config, InputStream in)
        throws IOException
    {
        return config.mLauncher.open(false, in);
    }

    /**
     * Writes a cache priming set into the given stream, which can then be used later to
     * {@linkplain #applyCachePrimer prime} the cache.
     *
     * @param out cache priming destination; buffering is recommended; not auto-closed
     * @see DatabaseConfig#cachePriming
     */
    public void createCachePrimer(OutputStream out) throws IOException;

    /**
     * Prime the cache, from a set encoded {@linkplain #createCachePrimer earlier}.
     *
     * @param in caching priming source; buffering is recommended; auto-closed
     * @see DatabaseConfig#cachePriming
     */
    public void applyCachePrimer(InputStream in) throws IOException;

    /**
     * Returns an object for enabling remote access into this database. As long as the server
     * is still open, the JVM won't exit. Closing the database will also close the server.
     *
     * <p>If the database is configured with {@link DatabaseConfig#replicate replication},
     * remote access is already enabled, and so a server doesn't need to be created.
     *
     * @see #connect connect
     */
    public Server newServer() throws IOException;

    /**
     * Returns a collection of database statistics.
     */
    public DatabaseStats stats();

    /**
     * Flushes all committed transactions, but not durably. Transactions committed with
     * {@linkplain DurabilityMode#NO_FLUSH no-flush} effectively become {@linkplain
     * DurabilityMode#NO_SYNC no-sync} durable.
     *
     * <p>When the database is replicated, the no-flush mode is identical to the no-sync mode.
     * Calling this method on a replicated database has no effect.
     */
    @Override
    public void flush() throws IOException;

    /**
     * Durably flushes all committed transactions. Transactions committed with {@linkplain
     * DurabilityMode#NO_FLUSH no-flush} and {@linkplain DurabilityMode#NO_SYNC no-sync}
     * effectively become {@linkplain DurabilityMode#SYNC sync} durable.
     */
    public void sync() throws IOException;

    /**
     * Durably sync and checkpoint all changes to the database. In addition to ensuring that
     * all committed transactions are durable, checkpointing ensures that non-transactional
     * modifications are durable. Checkpoints are performed automatically by a background
     * thread, at a {@linkplain DatabaseConfig#checkpointRate configurable} rate.
     */
    public void checkpoint() throws IOException;

    /**
     * Temporarily suspend automatic checkpoints and wait for any in-progress checkpoint to
     * complete. Suspend may be invoked multiple times, but each must be paired with a
     * {@linkplain #resumeCheckpoints resume} call to enable automatic checkpoints again.
     *
     * @throws IllegalStateException if suspended more than 2<sup>31</sup> times
     */
    public void suspendCheckpoints();

    /**
     * Resume automatic checkpoints after having been temporarily {@link #suspendCheckpoints
     * suspended}.
     *
     * @throws IllegalStateException if resumed more than suspended
     */
    public void resumeCheckpoints();

    /**
     * Returns the checkpoint commit lock, which can be held to prevent checkpoints from
     * capturing a safe commit point. By holding the commit lock, multiple modifications can be
     * made to the database atomically, even without using a transaction.
     *
     * <p>The commit lock should only ever be held briefly, because it prevents checkpoints
     * from starting. Holding the commit lock can stall other threads trying to make
     * modifications, if a checkpoint is trying to start. In addition, a thread holding the
     * commit lock must not attempt to issue a checkpoint, because deadlock is possible.
     */
    public Lock commitLock();

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
    public boolean compactFile(CompactionObserver observer, double target)
        throws IOException;

    /**
     * Verifies the integrity of the database and all indexes. Using multiple threads speeds up
     * verification, even though some nodes might be visited multiple times.
     *
     * @param observer optional observer; pass null for default
     * @param numThreads pass 0 for default, or if negative, the actual number will be {@code
     * (-numThreads * availableProcessors)}.
     * @return true if verification passed
     */
    public boolean verify(VerificationObserver observer, int numThreads) throws IOException;

    /**
     * Returns true if the database instance is currently the leader.
     */
    public boolean isLeader();

    /**
     * Registers the given task to start in a separate thread when the database instance has
     * become the leader. If already the leader, the task is started immediately. The task is
     * started at most once per registration, and a second task is called when leadership is
     * lost. It too is called in a separate thread, and at most once per registration. Note
     * that if leadership is quickly lost, the second task might run before the first task.
     *
     * @param acquired called when leadership is acquired (can be null)
     * @param lost called when leadership is lost (can be null)
     */
    public void uponLeader(Runnable acquired, Runnable lost);

    /**
     * If the database instance is currently acting as a leader, attempt to give up leadership
     * and become a replica. If the database is a replica, or if failover is successful, true
     * is returned. When false is returned, the database is likely still the leader, either
     * because the database isn't replicated, or because no replicas exist to failover to.
     */
    public boolean failover() throws IOException;

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
     * @param cause if non-null, delivers a {@linkplain EventType#PANIC_UNHANDLED_EXCEPTION
     * panic} event and future database accesses will rethrow the cause
     * @see #shutdown
     */
    @Override
    public void close(Throwable cause) throws IOException;

    /**
     * Returns true if database was explicitly closed, or if it was closed due to a panic.
     */
    public boolean isClosed();

    /**
     * Cleanly closes the database, ensuring durability of all modifications. A checkpoint is
     * issued first, and so a quick recovery is performed when the database is re-opened. As a
     * side effect of shutting down, all extraneous files are deleted.
     */
    public void shutdown() throws IOException;
}
