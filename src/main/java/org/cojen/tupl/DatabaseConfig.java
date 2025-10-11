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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import java.util.Map;

import java.util.concurrent.TimeUnit;

import java.util.function.Supplier;

import java.util.zip.Checksum;

import org.cojen.tupl.core.Launcher;

import org.cojen.tupl.diag.EventListener;

import org.cojen.tupl.ext.Crypto;
import org.cojen.tupl.ext.CustomHandler;
import org.cojen.tupl.ext.PrepareHandler;

import org.cojen.tupl.io.MappedPageArray;
import org.cojen.tupl.io.OpenOption;
import org.cojen.tupl.io.PageArray;
import org.cojen.tupl.io.PageCompressor;
import org.cojen.tupl.io.StripedPageArray;

import org.cojen.tupl.repl.ReplicatorConfig;
import org.cojen.tupl.repl.StreamReplicator;

/**
 * Configuration options used when {@linkplain Database#open opening} a database.
 *
 * @author Brian S O'Neill
 */
public class DatabaseConfig implements Cloneable {
    // Contains all the actual configuration options.
    final Launcher mLauncher;

    public DatabaseConfig() {
        mLauncher = new Launcher();
    }

    private DatabaseConfig(Launcher launcher) {
        mLauncher = launcher;
    }

    /**
     * Set the base file name for the database, which must reside in an
     * ordinary file directory. If no base file is provided, database is
     * non-durable and cannot exceed the size of the cache.
     */
    public DatabaseConfig baseFile(File file) {
        mLauncher.baseFile(file == null ? null : file.getAbsoluteFile());
        return this;
    }

    /**
     * Set the base file name for the database, which must reside in an
     * ordinary file directory. If no base file is provided, database is
     * non-durable and cannot exceed the size of the cache.
     */
    public DatabaseConfig baseFilePath(String path) {
        return baseFile(path == null ? null : new File(path));
    }

    /**
     * Set true to create directories for the base and data file, if they don't
     * already exist. Default is true.
     */
    public DatabaseConfig createFilePath(boolean mkdirs) {
        mLauncher.createFilePath(mkdirs);
        return this;
    }

    /**
     * Set the data file for the database, which by default resides in the same
     * directory as the base file. The data file can be in a separate
     * directory, and it can even be a raw block device.
     */
    public DatabaseConfig dataFile(File file) {
        return dataFiles(file);
    }

    /**
     * Stripe the database data file across several files, expected to be on
     * separate devices. The data files can refer to ordinary files or to raw
     * block devices.
     */
    public DatabaseConfig dataFiles(File... files) {
        if (files != null && files.length != 0) {
            var dataFiles = new File[files.length];
            for (int i=0; i<files.length; i++) {
                dataFiles[i] = files[i].getAbsoluteFile();
            }
            files = dataFiles;
        }
        mLauncher.dataFiles(files);
        return this;
    }

    /**
     * Enable memory mapping of the data files. Not recommended for 32-bit platforms or for
     * databases which don't fit entirely in main memory. Memory mapped files tend to exhibit
     * poor performance when they don't fit in main memory.
     *
     * <p>If the data file is fixed in size, consider calling {@link #dataPageArray
     * dataPageArray} with {@link MappedPageArray} for best memory mapping performance. Combine
     * with {@link StripedPageArray} when using multiple data files.
     */
    public DatabaseConfig mapDataFiles(boolean mapped) {
        mLauncher.mapDataFiles(mapped);
        return this;
    }

    /**
     * Use a custom storage layer instead of the default data file.
     */
    public DatabaseConfig dataPageArray(PageArray array) {
        mLauncher.dataPageArray(array);
        return this;
    }

    /**
     * Use a custom storage layer instead of the default data file.
     *
     * @param factory creates a new PageArray
     */
    public DatabaseConfig dataPageArray(Supplier<? extends PageArray> factory) {
        mLauncher.dataPageArray(factory.get());
        return this;
    }

    /**
     * Set the minimum cache size, overriding the default.
     *
     * @param minBytes cache size, in bytes
     */
    public DatabaseConfig minCacheSize(long minBytes) {
        mLauncher.minCacheSize(minBytes);
        return this;
    }

    /**
     * Set the maximum cache size, overriding the default.
     *
     * @param maxBytes cache size, in bytes
     */
    public DatabaseConfig maxCacheSize(long maxBytes) {
        mLauncher.maxCacheSize(maxBytes);
        return this;
    }

    /**
     * Convenience method which sets the minimum and maximum cache size, overriding the default.
     *
     * @param size cache size, in bytes
     */
    public DatabaseConfig cacheSize(long size) {
        minCacheSize(size);
        maxCacheSize(size);
        return this;
    }

    /**
     * Set the default transaction durability mode, which is {@link
     * DurabilityMode#SYNC SYNC} if not overridden. If database itself is
     * non-durable, durability modes are ignored.
     */
    public DatabaseConfig durabilityMode(DurabilityMode durabilityMode) {
        mLauncher.durabilityMode(durabilityMode);
        return this;
    }

    /**
     * Set the default lock upgrade rule, which is {@link LockUpgradeRule#STRICT STRICT} if not
     * overridden.
     */
    public DatabaseConfig lockUpgradeRule(LockUpgradeRule lockUpgradeRule) {
        mLauncher.lockUpgradeRule(lockUpgradeRule);
        return this;
    }

    /**
     * Set the default lock acquisition timeout, which is 1 second if not
     * overridden. A negative timeout is infinite.
     *
     * @param unit required unit if timeout is more than zero
     */
    public DatabaseConfig lockTimeout(long timeout, TimeUnit unit) {
        mLauncher.lockTimeout(timeout, unit);
        return this;
    }

    /**
     * Set the rate at which {@linkplain Database#checkpoint checkpoints} are automatically
     * performed. Default rate is 1 second. Pass a negative value to disable automatic
     * checkpoints.
     *
     * @param unit required unit if rate is more than zero
     */
    public DatabaseConfig checkpointRate(long rate, TimeUnit unit) {
        mLauncher.checkpointRate(rate, unit);
        return this;
    }

    /**
     * Set the minimum redo log size required for an automatic {@linkplain Database#checkpoint
     * checkpoint} to actually be performed. Default is 100 MiB. If database is used primarily
     * for non-transactional operations, the threshold should be set to zero.
     */
    public DatabaseConfig checkpointSizeThreshold(long bytes) {
        mLauncher.checkpointSizeThreshold(bytes);
        return this;
    }

    /**
     * Set the maximum delay before an automatic {@linkplain Database#checkpoint checkpoint} is
     * performed, regardless of the redo log size threshold. Default is 1 minute, and a
     * negative delay is infinite. If database is used primarily for non-transactional
     * operations, the threshold should be set to zero.
     *
     * @param unit required unit if delay is more than zero
     */
    public DatabaseConfig checkpointDelayThreshold(long delay, TimeUnit unit) {
        mLauncher.checkpointDelayThreshold(delay, unit);
        return this;
    }

    /**
     * Specify the maximum number of threads for performing checkpointing, to speed it up. This
     * option is most useful when combined with the {@link #syncWrites syncWrites} option, or
     * when using {@link OpenOption#DIRECT_IO DIRECT_IO}. The default number of threads is
     * one. If a negative number is provided, the actual number applied is {@code (-num *
     * availableProcessors)}.
     */
    public DatabaseConfig maxCheckpointThreads(int num) {
        mLauncher.maxCheckpointThreads(num);
        return this;
    }

    /**
     * Set a listener which receives notifications of actions being performed by the
     * database. Listener implementation must be thread-safe.
     */
    public DatabaseConfig eventListener(EventListener listener) {
        mLauncher.eventListener(listener);
        return this;
    }

    /**
     * Set multiple listeners which receive notifications of actions being performed by the
     * database. Listener implementations must be thread-safe.
     */
    public DatabaseConfig eventListeners(EventListener... listeners) {
        mLauncher.eventListeners(listeners);
        return this;
    }

    /**
     * Set true to ensure all writes to the main database file are immediately durable,
     * although not checkpointed. This option typically reduces overall performance, but
     * checkpoints complete more quickly. As a result, the main database file requires less
     * pre-allocated pages and is smaller. Also consider specifying more {@linkplain
     * #maxCheckpointThreads checkpoint threads} when using this option.
     */
    public DatabaseConfig syncWrites(boolean fileSync) {
        mLauncher.syncWrites(fileSync);
        return this;
    }

    /**
     * Open the database file in read only mode. Writes to the database are permitted until the
     * cache fills up, but nothing is persisted.
     */
    public DatabaseConfig readOnly(boolean readOnly) {
        mLauncher.readOnly(readOnly);
        return this;
    }

    /**
     * Set the page size, which is 4096 bytes by default.
     */
    public DatabaseConfig pageSize(int size) {
        mLauncher.pageSize(size);
        return this;
    }

    /**
     * Enable automatic cache priming, which writes a priming set into a special file when the
     * process is cleanly shutdown. When opened again, the priming set is applied and the file
     * is deleted. Option has no effect if database is non-durable.
     *
     * @see Database#createCachePrimer
     */
    public DatabaseConfig cachePriming(boolean priming) {
        mLauncher.cachePriming(priming);
        return this;
    }

    /**
     * When the process is cleanly shutdown, attempt to issue a full database shutdown. This
     * ensures full durability of all modifications. Setting this option prevents the process
     * from exiting until a final checkpoint completes.
     */
    public DatabaseConfig cleanShutdown(boolean shutdown) {
        mLauncher.cleanShutdown(shutdown);
        return this;
    }

    /**
     * Enable replication using the given configuration. When the database is opened, the given
     * config object is cloned and the base file and event listener are assigned to it.
     */
    public DatabaseConfig replicate(ReplicatorConfig config) {
        mLauncher.replicate(config);
        return this;
    }

    /**
     * Enable replication with an explicit {@link StreamReplicator} instance.
     */
    public DatabaseConfig replicate(StreamReplicator repl) {
        mLauncher.replicate(repl);
        return this;
    }

    /**
     * If replication is enabled, specify the maximum number of threads to process incoming
     * changes. Default is the number of available processors. If a negative number is
     * provided, the actual number applied is {@code (-num * availableProcessors)}. When the
     * local member is the leader, these threads are used for background processing of
     * transactions which weren't committed with {@code SYNC} durability.
     *
     * <p>If replication isn't enabled, this option controls the number of threads used to
     * recover transactions from the redo log during startup.
     */
    public DatabaseConfig maxReplicaThreads(int num) {
        mLauncher.maxReplicaThreads(num);
        return this;
    }

    public DatabaseConfig enableJMX(boolean enable) {
        mLauncher.enableJMX(enable);
        return this;
    }

    /**
     * Enable full encryption of the data files, transaction logs, snapshots, and cache priming
     * sets. Option has no effect if database is non-durable. If replication is enabled,
     * encryption is not applied to the replication stream. A {@link StreamReplicator}
     * implementation must perform its own encryption.
     *
     * <p>Allocated but never used pages within the data files are unencrypted, although they
     * contain no information. Temporary files used by in-progress snapshots contain encrypted
     * content.
     *
     * @param factory creates a new crypto instance
     */
    public DatabaseConfig encrypt(Supplier<? extends Crypto> factory) {
        mLauncher.encrypt(factory.get());
        return this;
    }

    /**
     * Enable 32-bit checksums for all of the underlying database pages. The page size reported
     * by the database will be 4 bytes smaller, to make room for the checksum.
     *
     * @param factory creates new checksum instances; {@code CRC32C::new} is recommended
     */
    public DatabaseConfig checksumPages(Supplier<? extends Checksum> factory) {
        mLauncher.checksumPages(factory);
        return this;
    }

    /**
     * Compress the underlying database pages, reducing overall size at the cost of
     * performance. To be effective, the given full page size must be larger than physical page
     * size. A full page size of 65536 bytes paired with the default physical page size of 4096
     * bytes achieves the best compression. A full page size which is smaller than the physical
     * page size is generally a poor choice.
     *
     * <p>The compression layer itself needs its own cache when accessing pages, although it
     * can be much smaller than the primary cache size. Setting it to be 1% of the primary
     * cache size should be sufficient. For a non-durable database, the compression cache
     * should be much larger to avoid running out of space.
     *
     * @param fullPageSize full size of pages when uncompressed
     * @param cacheSize cache size (in bytes) for the compression layer
     * @param factory creates new page compressor instances
     */
    public DatabaseConfig compressPages(int fullPageSize, long cacheSize,
                                        Supplier<? extends PageCompressor> factory)
    {
        mLauncher.compressPages(fullPageSize, cacheSize, factory);
        return this;
    }

    /**
     * Provide handlers for recovering custom transactional operations. The name assigned to
     * each handler must be unique and never change.
     */
    public DatabaseConfig customHandlers(Map<String, ? extends CustomHandler> handlers) {
        mLauncher.customHandlers(handlers);
        return this;
    }

    /**
     * Provide handlers for recovering prepared transactions. The name assigned to each handler
     * must be unique and never change.
     */
    public DatabaseConfig prepareHandlers(Map<String, ? extends PrepareHandler> handlers) {
        mLauncher.prepareHandlers(handlers);
        return this;
    }

    /**
     * Opens the database in read-only mode for debugging purposes, and then closes it. The
     * format of the printed messages and the supported properties are subject to change.
     *
     * <ul>
     * <li>traceUndo=true to print all recovered undo log messages
     * <li>traceRedo=true to print all recovered redo log messages
     * </ul>
     *
     * @param out pass null to print to standard out
     * @param properties optional
     */
    public void debugOpen(PrintStream out, Map<String, ?> properties)
        throws IOException
    {
        mLauncher.debugOpen(out, properties);
    }

    @Override
    public DatabaseConfig clone() {
        return new DatabaseConfig(mLauncher.clone());
    }
}
