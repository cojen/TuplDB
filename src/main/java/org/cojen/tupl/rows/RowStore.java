/*
 *  Copyright 2021 Cojen.org
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

package org.cojen.tupl.rows;

import java.io.IOException;

import java.lang.ref.WeakReference;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.cojen.tupl.CorruptDatabaseException;
import org.cojen.tupl.Cursor;
import org.cojen.tupl.Database;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UniqueConstraintException;
import org.cojen.tupl.View;

import org.cojen.tupl.core.CoreDatabase;
import org.cojen.tupl.core.ScanVisitor;

import org.cojen.tupl.util.Runner;

import static org.cojen.tupl.rows.RowUtils.*;

/**
 * Main class for managing row persistence via tables.
 *
 * @author Brian S O'Neill
 */
public class RowStore {
    private final WeakReference<RowStore> mSelfRef;
    final CoreDatabase mDatabase;

    /* Schema metadata for all types.

       (indexId) ->
         int current schemaVersion,
         ColumnSet[] alternateKeys, (a type of secondary index)
         ColumnSet[] secondaryIndexes

       (indexId, schemaVersion) -> primary ColumnSet

       (indexId, hash(primary ColumnSet)) -> schemaVersion[]    // hash collision chain

       (indexId, 0, K_SECONDARY, descriptor) -> indexId, state

       (indexId, 0, K_TYPE_NAME) -> current type name (UTF-8)

       (0L, indexId, taskType) -> ...  workflow task against an index

       The schemaVersion is limited to 2^31, and the hash is encoded with bit 31 set,
       preventing collisions. In addition, schemaVersion cannot be 0, and so the extended keys
       won't collide, although the longer overall key length prevents collisions as well.

       Because indexId 0 is reserved (for the registry), it won't be used for row storage, and
       so it can be used for tracking workflow tasks.
     */
    private final Index mSchemata;

    private final WeakCache<Index, TableManager<?>> mTableManagers;

    // Extended key for referencing secondary indexes.
    private static final int K_SECONDARY = 1;

    // Extended key to store the fully qualified type name.
    private static final int K_TYPE_NAME = 2;

    /*
      FIXME: notes

      - For indexes to delete, acquire a lock to delete them. If state is backfill, then stop
        any backfill task, and delete the index. If state is active, then cannot delete until all
        queries using the index are done with it.

      - Replicas cannot delete indexes. Only the leader can. However, the leader cannot delete
        an index if any replica is still using it. Hmm... there's no way to ask this. Need to
        define a new redo operation which deletes an index when no longer in use. Using GC
        takes too long. Need a latch or clutch to manage a ref count.

      - What if the replica becomes the leader and has a different set of index annotations?
        Should it revert? This is a problem during deployments. Should index definitions not be
        defined as annotations? Won't really help if old code is still running. Is an explicit
        index set version required to be manually defined? I think in practice it should be
        fine for indexes to flip on and off during a deployment. Everything should be fine when
        the dust settles.

     */

    private static final int TASK_DELETE_SCHEMA = 1;

    public RowStore(CoreDatabase db, Index schemata) throws IOException {
        mSelfRef = new WeakReference<>(this);
        mDatabase = db;
        mSchemata = schemata;
        mTableManagers = new WeakCache<>();

        registerToUpdateSchemata();

        // Finish any tasks left over from when the RowStore was last used.
        finishAllWorkflowTasks();
    }

    WeakReference<RowStore> ref() {
        return mSelfRef;
    }

    public Index schemata() {
        return mSchemata;
    }

    /**
     * Scans the schemata and all table secondary indexes. Doesn't scan table primary indexes.
     */
    public void scanAllIndexes(ScanVisitor visitor) throws IOException {
        visitor.apply(mSchemata);

        try (Cursor c = mSchemata.newCursor(null)) {
            byte[] key;
            for (c.first(); (key = c.key()) != null; ) {
                if (key.length <= 8) {
                    c.next();
                    continue;
                }
                long indexId = decodeLongBE(key, 0);
                if (indexId == 0 ||
                    decodeIntBE(key, 8) != 0 || decodeIntBE(key, 8 + 4) != K_SECONDARY)
                {
                    if (++indexId == 0) {
                        break;
                    }
                    c.findNearbyGe(key(indexId));
                } else {
                    Index secondaryIndex = mDatabase.indexById(decodeLongLE(c.value(), 0));
                    if (secondaryIndex != null) {
                        visitor.apply(secondaryIndex);
                    }
                    c.next();
                }
            }
        }
    }

    private TableManager<?> tableManager(Index ix) {
        var manager = (TableManager<?>) mTableManagers.get(ix);

        if (manager == null) {
            synchronized (mTableManagers) {
                manager = (TableManager<?>) mTableManagers.get(ix);
                if (manager == null) {
                    manager = new TableManager<>(ix);
                    mTableManagers.put(ix, manager);
                }
            }
        }

        return manager;
    }

    @SuppressWarnings("unchecked")
    public <R> Table<R> asTable(Index ix, Class<R> type) throws IOException {
        return ((TableManager<R>) tableManager(ix)).asTable(this, ix, type);
    }

    /**
     * Called by TableManager via asTable.
     */
    @SuppressWarnings("unchecked")
    <R> AbstractTable<R> makeTable(TableManager<R> manager, Index ix, Class<R> type)
        throws IOException
    {
        // Throws an exception if type is malformed.
        RowInfo info = RowInfo.find(type);

        AbstractTable<R> table;

        // Can use NO_FLUSH because transaction will be only used for reading data.
        Transaction txn = mSchemata.newTransaction(DurabilityMode.NO_FLUSH);
        try {
            txn.lockMode(LockMode.REPEATABLE_READ);

            // With a txn lock held, check if the primary key definition has changed.
            RowInfo currentInfo = decodeExisting(txn, null, ix.id());
            if (currentInfo != null) {
                if (!info.keyColumns.equals(currentInfo.keyColumns)) {
                    // FIXME: Better exception.
                    throw new IllegalStateException("Cannot alter primary key: " + type.getName());
                }
                // FIXME: Also check that alt key and secondary definitions haven't changed.
            }

            try {
                var mh = new TableMaker(this, type, info.rowGen(), ix.id()).finish();
                table = (AbstractTable) mh.invoke(manager, ix);
            } catch (Throwable e) {
                throw rethrow(e);
            }
        } finally {
            txn.reset();
        }

        // Attempt to eagerly update schema metadata and secondary indexes.
        try {
            // Pass false for notify because examineSecondaries will be invoked by caller.
            schemaVersion(info, ix.id(), false);
        } catch (IOException e) {
            // Ignore and try again when storing rows or when the leadership changes.
        }

        return table;
    }

    public <R> Table<R> alternateKeyTable(AbstractTable<R> primaryTable, String... columns)
        throws IOException
    {
        return indexTable(primaryTable, true, columns);
    }

    public <R> Table<R> secondaryIndexTable(AbstractTable<R> primaryTable, String... columns)
        throws IOException
    {
        return indexTable(primaryTable, false, columns);
    }

    @SuppressWarnings("unchecked")
    private <R> Table<R> indexTable(AbstractTable<R> primaryTable, boolean alt, String... columns)
        throws IOException
    {
        // FIXME: Cache it, normalize descriptor key.

        Class<R> rowType = primaryTable.rowType();
        RowInfo rowInfo = RowInfo.find(rowType);
        ColumnSet cs = rowInfo.examineIndex(null, columns, alt);

        if (cs == null) {
            return null;
        }

        var encoder = new Encoder(columns.length * 16);
        char type = alt ? 'A' : 'I';
        byte[] search = EncodedRowInfo.encodeDescriptor(type, encoder, cs);

        View secondariesView = viewExtended(primaryTable.mSource.id(), K_SECONDARY);
        secondariesView = secondariesView.viewPrefix(new byte[] {(byte) type}, 0);

        // Identify the first match. The ASCII value for '+' is less than '-', and so a match
        // for ascending order is generally found first when an unspecified order was given.
        long indexId;
        RowInfo indexRowInfo;
        byte[] descriptor;
        find: {
            try (Cursor c = secondariesView.newCursor(null)) {
                for (c.first(); c.key() != null; c.next()) {
                    if (!descriptorMatches(search, c.key())) {
                        continue;
                    }
                    // FIXME: return null if not active
                    indexId = decodeLongLE(c.value(), 0);
                    indexRowInfo = indexRowInfo(RowInfo.find(rowType), c.key());
                    descriptor = c.key();
                    break find;
                }
            }
            return null;
        }

        Index ix = mDatabase.indexById(indexId);

        if (ix == null) {
            return null;
        }

        // Indexes don't have indexes.
        indexRowInfo.alternateKeys = Collections.emptyNavigableSet();
        indexRowInfo.secondaryIndexes = Collections.emptyNavigableSet();

        var manager = (TableManager<R>) tableManager(ix);

        AbstractTable table;

        try {
            var maker = new TableMaker
                (this, rowType, rowInfo.rowGen(), indexRowInfo.rowGen(), descriptor, ix.id());
            var mh = maker.finish();
            table = (AbstractTable) mh.invoke(manager, ix);
        } catch (Throwable e) {
            throw rethrow(e);
        }

        return table;
    }

    /**
     * Compares descriptors as encoded by EncodedRowInfo.encodeDescriptor.
     *
     * @param search can have '~' unspecified orderings
     * @param found should not have any unspecified orderings
     */
    private static boolean descriptorMatches(byte[] search, byte[] found) {
        if (search.length != found.length) {
            return false;
        }

        int offset = 1; // skip the type prefix

        for (int part=1; part<=2; part++) {
            int numColumns = decodePrefixPF(search, offset);
            if (numColumns != decodePrefixPF(found, offset)) {
                return false;
            }
            offset += lengthPrefixPF(numColumns);

            for (int i=0; i<numColumns; i++) {
                if (part == 1) { // first part examines the "keys" portion of the descriptor
                    byte ordering = search[offset];
                    if (ordering != found[offset] && ordering != '~') {
                        return false;
                    }
                    offset++;
                }
                int nameLength = decodePrefixPF(search, offset);
                if (nameLength != decodePrefixPF(found, offset)) {
                    return false;
                }
                if (!Arrays.equals(search, offset, offset + nameLength,
                                   found, offset, offset + nameLength))
                {
                    return false;
                }
                offset += lengthPrefixPF(nameLength);
                offset += nameLength;
            }
        }

        return offset == search.length; // should always be true unless descriptor is malformed
    }

    private void registerToUpdateSchemata() {
        // The second task is invoked when leadership is lost, which sets things up for when
        // leadership is acquired again.
        mDatabase.uponLeader(this::updateSchemata, this::registerToUpdateSchemata);
    }

    /**
     * Called when database has become the leader, providing an opportunity to update the
     * schema for all tables currently in use.
     */
    @SuppressWarnings("unchecked")
    private void updateSchemata() {
        List<TableManager<?>> managers = mTableManagers.copyValues();

        if (managers != null) {
            try {
                for (var manager : managers) {
                    for (Class<?> type : manager.mTables.copyKeys(Class[]::new)) {
                        schemaVersion(RowInfo.find(type), manager.mPrimaryIndex.id(), true);
                    }
                }
            } catch (IOException e) {
                // Ignore and try again when storing rows or when the leadership changes.
            } catch (Throwable e) {
                uncaught(e);
            }
        }
    }

    private void uncaught(Throwable e) {
        if (!mDatabase.isClosed()) {
            RowUtils.uncaught(e);
        }
    }

    /**
     * Called in response to a redo log message. Implementation should examine the set of
     * secondary indexes associated with the table and perform actions to build or drop them.
     */
    public void notifySchema(long indexId) {
        // FIXME: If there's an exception, then the redo op isn't processed again. Define a
        // workflow task perhaps?

        // Must launch from a separate thread because locks are held by this thread until the
        // transaction finishes.
        Runner.start(() -> doNotifySchema(indexId));
    }

    private void doNotifySchema(long indexId) {
        try {
            Index ix = mDatabase.indexById(indexId);
            examineSecondaries(tableManager(ix));
        } catch (Throwable e) {
            uncaught(e);
            return;
        }
    }

    /**
     * Also called from TableManager.asTable.
     */
    <R> void examineSecondaries(TableManager<R> manager) throws IOException {
        long indexId = manager.mPrimaryIndex.id();

        // Can use NO_FLUSH because transaction will be only used for reading data.
        Transaction txn = mDatabase.newTransaction(DurabilityMode.NO_FLUSH);
        try {
            txn.lockTimeout(-1, null);

            // Lock to prevent changes and to allow one thread to call examineSecondaries.
            mSchemata.lockUpgradable(txn, key(indexId));

            txn.lockMode(LockMode.READ_COMMITTED);

            manager.update(this, txn, viewExtended(indexId, K_SECONDARY));
        } finally {
            txn.reset();
        }
    }

    /**
     * Called by IndexBackfill when an index backfill has finished.
     */
    void activateSecondaryIndex(IndexBackfill backfill, boolean success) throws IOException {
        long indexId = backfill.mManager.mPrimaryIndex.id();
        long secondaryId = backfill.mSecondaryIndex.id();

        boolean activated = false;

        // Use NO_REDO it because this method can be called by a replica.
        Transaction txn = mDatabase.newTransaction(DurabilityMode.NO_REDO);
        try {
            txn.lockTimeout(-1, null);

            // Lock to prevent changes and to allow one thread to call examineSecondaries.
            mSchemata.lockUpgradable(txn, key(indexId));

            View secondariesView = viewExtended(indexId, K_SECONDARY);

            if (success) {
                try (Cursor c = secondariesView.newCursor(txn)) {
                    c.find(backfill.mSecondaryDescriptor);
                    byte[] value = c.value();
                    if (value != null && value[8] == 'B' && decodeLongLE(value, 0) == secondaryId) {
                        // Switch to "active" state.
                        value[8] = 'A';
                        c.store(value);
                        activated = true;
                    }
                }
            }

            backfill.mManager.update(this, txn, secondariesView);

            txn.commit();
        } finally {
            txn.reset();
        }

        if (activated) {
            // With NO_REDO, need a checkpoint to ensure durability. If the database is closed
            // before it finishes, the whole backfill process starts over when the database is
            // later reopened.
            mDatabase.checkpoint();
        }
    }

    /**
     * Should be called when the index is being dropped. Does nothing if index wasn't used for
     * storing rows.
     *
     * This method should be called with the shared commit lock held, and it
     * non-transactionally stores task metadata which indicates that the schema should be
     * deleted. The caller should run the returned object without holding the commit lock.
     *
     * If no checkpoint occurs, then the expecation is that the deleteSchema method is called
     * again, which allows the deletion to run again. This means that deleteSchema should
     * only really be called from removeFromTrash, which automatically runs again if no
     * checkpoint occurs.
     *
     * @param indexKey long index id, big-endian encoded
     * @return an optional task to run without commit lock held
     */
    public Runnable deleteSchema(byte[] indexKey) throws IOException {
        var taskKey = new byte[8 + 8 + 4];
        System.arraycopy(indexKey, 0, taskKey, 8, 8);
        encodeIntBE(taskKey, 8 + 8, TASK_DELETE_SCHEMA);

        // Use a transaction to lock the task, and so finishAllWorkflowTasks will skip it.
        Transaction txn = mDatabase.newTransaction(DurabilityMode.NO_REDO);
        try {
            mSchemata.store(txn, taskKey, EMPTY_BYTES);
        } catch (Throwable e) {
            txn.reset(e);
            throw e;
        }

        return () -> {
            try {
                try {
                    doDeleteSchema(indexKey);
                    mSchemata.delete(txn, taskKey);
                    txn.commit();
                } finally {
                    txn.reset();
                }
            } catch (IOException e) {
                throw rethrow(e);
            }
        };
    }

    private void finishAllWorkflowTasks() throws IOException {
        // Use transaction locks to identity tasks which should be skipped.
        Transaction txn = mDatabase.newTransaction(DurabilityMode.NO_REDO);
        try {
            txn.lockMode(LockMode.UNSAFE); // don't auto acquire locks

            var prefix = new byte[8]; // 0L is the prefix for all workflow tasks

            try (Cursor c = mSchemata.viewPrefix(prefix, 0).newCursor(txn)) {
                c.autoload(false);

                for (c.first(); c.key() != null; c.next()) {
                    if (!txn.tryLockExclusive(mSchemata.id(), c.key(), 0).isHeld()) {
                        // Skip it.
                        continue;
                    }

                    try {
                        // Load and check the value again, in case another thread deleted it
                        // before the lock was acquired.
                        c.load();
                        byte[] taskValue = c.value();
                        if (taskValue != null) {
                            doWorkflowTask(c.key(), taskValue);
                            c.delete();
                        }
                    } finally {
                        txn.unlock();
                    }
                }
            }
        } finally {
            txn.reset();
        }
    }

    /**
     * @param taskKey (0L, indexId, taskType)
     */
    private void doWorkflowTask(byte[] taskKey, byte[] taskValue) throws IOException {
        var indexKey = new byte[8];
        System.arraycopy(taskKey, 8, indexKey, 0, 8);
        int taskType = decodeIntBE(taskKey, 8 + 8);

        switch (taskType) {
        default:
            throw new CorruptDatabaseException("Unknown task: " + taskType);
        case TASK_DELETE_SCHEMA:
            doDeleteSchema(indexKey);
            break;
        }
    }

    private void doDeleteSchema(byte[] indexKey) throws IOException {
        try (Cursor c = mSchemata.viewPrefix(indexKey, 0).newCursor(Transaction.BOGUS)) {
            c.autoload(false);
            for (c.first(); c.key() != null; c.next()) {
                // FIXME: If K_SECONDARY, delete the indexes.
                c.delete();
            }
        }
    }

    /**
     * Returns the schema version for the given row info, creating a new version if necessary.
     *
     * @param notify true to call notifySchema if anything changed
     */
    int schemaVersion(final RowInfo info, final long indexId, boolean notify) throws IOException {
        int schemaVersion;

        Transaction txn = mSchemata.newTransaction(DurabilityMode.SYNC);
        try (Cursor current = mSchemata.newCursor(txn)) {
            txn.lockTimeout(-1, null);

            current.find(key(indexId));

            if (current.value() != null) {
                // Check if the currently defined schema matches.

                schemaVersion = decodeIntLE(current.value(), 0);
                RowInfo currentInfo = decodeExisting
                    (txn, info.name, indexId, current.value(), schemaVersion);

                if (info.matches(currentInfo)
                    && info.alternateKeysMatch(currentInfo)
                    && info.secondaryIndexesMatch(currentInfo))
                {
                    // Completely matches.
                    return schemaVersion;
                }

                if (!info.keyColumns.equals(currentInfo.keyColumns)) {
                    // FIXME: Better exception.
                    throw new IllegalStateException("Cannot alter primary key: " + info.name);
                }

                // FIXME: Also check that alt key and secondary definitions haven't changed.
            }

            // Find an existing schemaVersion or create a new one.

            final boolean isTempIndex = mDatabase.isInTrash(txn, indexId);

            if (isTempIndex) {
                // Temporary trees are always in the trash, and they don't replicate. For this
                // reason, don't attempt to replicate schema metadata either.
                txn.durabilityMode(DurabilityMode.NO_REDO);
            }

            final var encoded = new EncodedRowInfo(info);

            assignVersion: try (Cursor byHash = mSchemata.newCursor(txn)) {
                byHash.find(key(indexId, encoded.primaryHash | (1 << 31)));

                byte[] schemaVersions = byHash.value();
                if (schemaVersions != null) {
                    for (int pos=0; pos<schemaVersions.length; pos+=4) {
                        schemaVersion = decodeIntLE(schemaVersions, pos);
                        RowInfo existing = decodeExisting
                            (txn, info.name, indexId, null, schemaVersion);
                        if (info.matches(existing)) {
                            break assignVersion;
                        }
                    }
                }

                // Create a new version.

                View versionView = mSchemata.viewGt(key(indexId)).viewLt(key(indexId, 1 << 31));
                try (Cursor highest = versionView.newCursor(txn)) {
                    highest.autoload(false);
                    highest.last();

                    if (highest.value() == null) {
                        // First version.
                        schemaVersion = 1;
                    } else {
                        byte[] key = highest.key();
                        schemaVersion = decodeIntBE(key, key.length - 4) + 1;
                    }

                    highest.findNearby(key(indexId, schemaVersion));
                    highest.store(encoded.primaryData);
                }

                if (schemaVersions == null) {
                    schemaVersions = new byte[4];
                } else {
                    schemaVersions = Arrays.copyOfRange
                        (schemaVersions, 0, schemaVersions.length + 4);
                }

                encodeIntLE(schemaVersions, schemaVersions.length - 4, schemaVersion);
                byHash.store(schemaVersions);
            }

            encodeIntLE(encoded.currentData, 0, schemaVersion);
            current.store(encoded.currentData);

            // Store the type name, which is usually the same as the class name. In case the
            // table isn't currently open, this name can be used for reporting background
            // status updates. Although the index name could be used, it's not required to be
            // the same as the class name.
            View nameView = viewExtended(indexId, K_TYPE_NAME);
            try (Cursor c = nameView.newCursor(txn)) {
                c.find(EMPTY_BYTES);
                byte[] nameBytes = encodeStringUTF(info.name);
                if (!Arrays.equals(nameBytes, c.value())) {
                    c.store(nameBytes);
                }
            }

            // Start with the full set of secondary descriptors and later prune it down to
            // those that need to be created.
            var secondaries = encoded.secondaries;

            // Access a view of persisted secondary descriptors to index ids and states.
            View secondariesView = viewExtended(indexId, K_SECONDARY);

            // Find and update secondary indexes that should be deleted.
            try (Cursor c = secondariesView.newCursor(txn)) {
                for (c.first(); c.key() != null; c.next()) {
                    if (!secondaries.contains(c.key())) {
                        byte[] value = c.value();
                        if (value[8] != 'D') {
                            value[8] = 'D'; // "deleting" state
                            c.store(value);
                        }
                    }
                }
            }

            // Find and update secondary indexes to create.
            try (Cursor c = secondariesView.newCursor(txn)) {
                Iterator<byte[]> it = secondaries.iterator();
                while (it.hasNext()) {
                    c.findNearby(it.next());
                    byte[] value = c.value();
                    if (value != null) {
                        // Secondary index already exists.
                        it.remove();
                        // If state is "deleting", switch it to "backfill".
                        if (value[8] == 'D') {
                            value[8] = 'B';
                            c.store(value);
                        }
                    } else if (isTempIndex) {
                        // Secondary index doesn't exist, but temporary ones can be
                        // immediately created because they're not replicated.
                        it.remove();
                        value = new byte[8 + 1];
                        encodeLongLE(value, 0, mDatabase.newTemporaryIndex().id());
                        value[8] = 'B'; // "backfill" state
                        c.store(value);
                    }
                }
            }

            // The newly created index ids are copied into the array. Note that the array can
            // be empty, in which case calling createSecondaryIndexes is still necessary for
            // informing any replicas that potential changes have been made. The state of some
            // secondary indexes might have changed from "backfill" to "deleting", or vice
            // versa. If nothing changed, there's no harm in sending the notification anyhow.
            var ids = new long[secondaries.size()];

            // Transaction is committed as a side-effect.
            mDatabase.createSecondaryIndexes(txn, indexId, ids, () -> {
                try {
                    int i = 0;
                    for (byte[] desc : secondaries) {
                        byte[] value = new byte[8 + 1];
                        encodeLongLE(value, 0, ids[i++]);
                        value[8] = 'B'; // "backfill" state
                        if (!secondariesView.insert(txn, desc, value)) {
                            // Not expected.
                            throw new UniqueConstraintException();
                        }
                    }
                } catch (IOException e) {
                    rethrow(e);
                }
            });
        } finally {
            txn.reset();
        }

        if (notify) {
            // We're currently the leader, and so this method must be invoked directly.
            notifySchema(indexId);
        }

        return schemaVersion;
    }

    /**
     * Finds a RowInfo for a specific schemaVersion. If not the same as the current version,
     * the alternateKeys and secondaryIndexes will be null (not just empty sets).
     *
     * @throws CorruptDatabaseException if not found
     */
    RowInfo rowInfo(Class<?> rowType, long indexId, int schemaVersion)
        throws IOException, CorruptDatabaseException
    {
        // Can use NO_FLUSH because transaction will be only used for reading data.
        Transaction txn = mSchemata.newTransaction(DurabilityMode.NO_FLUSH);
        txn.lockMode(LockMode.REPEATABLE_READ);
        try (Cursor c = mSchemata.newCursor(txn)) {
            // Check if the indexId matches and the schemaVersion is the current one.
            c.autoload(false);
            c.find(key(indexId));
            RowInfo current = null;
            if (c.value() != null) {
                var buf = new byte[4];
                if (decodeIntLE(buf, 0) == schemaVersion) {
                    // Matches, but don't simply return it. The current one might not have been
                    // updated yet.
                    current = RowInfo.find(rowType);
                }
            }

            c.autoload(true);
            c.findNearby(key(indexId, schemaVersion));

            RowInfo info = decodeExisting(rowType.getName(), null, c.value());

            if (current != null && current.allColumns.equals(info.allColumns)) {
                // Current one matches, so use the canonical RowInfo instance.
                return current;
            } else if (info == null) {
                throw new CorruptDatabaseException
                    ("Schema version not found: " + schemaVersion + ", indexId=" + indexId);
            } else {
                return info;
            }
        } finally {
            txn.reset();
        }
    }

    /**
     * Decodes a RowInfo object for a secondary index, by parsing a binary descriptor which was
     * created by EncodedRowInfo.encodeDescriptor.
     */
    static SecondaryInfo indexRowInfo(RowInfo primaryInfo, byte[] desc) {
        byte type = desc[0];
        int offset = 1;

        var info = new SecondaryInfo(primaryInfo, type == 'A');

        int numKeys = decodePrefixPF(desc, offset);
        offset += lengthPrefixPF(numKeys);
        info.keyColumns = new LinkedHashMap<>(numKeys);

        for (int i=0; i<numKeys; i++) {
            boolean descending = desc[offset++] == '-';
            int nameLength = decodePrefixPF(desc, offset);
            offset += lengthPrefixPF(nameLength);
            String name = decodeStringUTF(desc, offset, nameLength).intern();
            offset += nameLength;

            ColumnInfo column = primaryInfo.allColumns.get(name);

            if (descending != column.isDescending()) {
                column = column.copy();
                column.typeCode ^= ColumnInfo.TYPE_DESCENDING;
            }

            info.keyColumns.put(name, column);
        }

        int numValues = decodePrefixPF(desc, offset);
        if (numValues == 0) {
            info.valueColumns = Collections.emptyNavigableMap();
        } else {
            offset += lengthPrefixPF(numValues);
            info.valueColumns = new TreeMap<>();

            for (int i=0; i<numValues; i++) {
                int nameLength = decodePrefixPF(desc, offset);
                offset += lengthPrefixPF(nameLength);
                String name = decodeStringUTF(desc, offset, nameLength).intern();
                offset += nameLength;

                ColumnInfo column = primaryInfo.allColumns.get(name);

                info.valueColumns.put(name, column);
            }
        }

        info.allColumns = new TreeMap<>(info.keyColumns);
        info.allColumns.putAll(info.valueColumns);

        return info;
    }

    /**
     * Decodes a set of secondary index RowInfo objects.
     */
    static SecondaryInfo[] indexRowInfos(RowInfo primaryInfo, byte[][] descriptors) {
        var infos = new SecondaryInfo[descriptors.length];
        for (int i=0; i<descriptors.length; i++) {
            infos[i] = indexRowInfo(primaryInfo, descriptors[i]);
        }
        return infos;
    }

    /**
     * @param key K_SECONDARY, etc
     */
    private View viewExtended(long indexId, int key) {
        var prefix = new byte[8 + 4 + 4];
        encodeLongBE(prefix, 0, indexId);
        encodeIntBE(prefix, 8 + 4, key);
        return mSchemata.viewPrefix(prefix, prefix.length);
    }

    /**
     * Decode the existing RowInfo for the current schema version.
     *
     * @param typeName can be null if unknown
     * @return null if not found
     */
    RowInfo decodeExisting(Transaction txn, String typeName, long indexId) throws IOException {
        byte[] currentData = mSchemata.load(txn, key(indexId));
        if (currentData == null) {
            return null;
        }
        return decodeExisting(txn, typeName, indexId, currentData, decodeIntLE(currentData, 0));
    }

    /**
     * Decode the existing RowInfo for the given schema version.
     *
     * @param typeName can be null if unknown
     * @param currentData can be null if not the current schema
     * @return null if not found
     */
    private RowInfo decodeExisting(Transaction txn, String typeName, long indexId,
                                   byte[] currentData, int schemaVersion)
        throws IOException
    {
        byte[] primaryData = mSchemata.load(txn, key(indexId, schemaVersion));

        if (typeName == null) {
            byte[] currentName = viewExtended(indexId, K_TYPE_NAME).load(txn, EMPTY_BYTES);
            if (currentName == null) {
                typeName = String.valueOf(indexId);
            } else {
                typeName = decodeStringUTF(currentName, 0, currentName.length);
            }
        }

        return decodeExisting(typeName, currentData, primaryData);
    }

    /**
     * @param currentData if null, then alternateKeys and secondaryIndexes won't be decoded
     * @param primaryData if null, then null is returned
     * @return null only if primaryData is null
     */
    private static RowInfo decodeExisting(String typeName, byte[] currentData, byte[] primaryData)
        throws CorruptDatabaseException
    {
        if (primaryData == null) {
            return null;
        }

        int pos = 0;
        int encodingVersion = primaryData[pos++] & 0xff;

        if (encodingVersion != 1) {
            throw new CorruptDatabaseException("Unknown encoding version: " + encodingVersion);
        }

        var info = new RowInfo(typeName);
        info.allColumns = new TreeMap<>();

        var names = new String[decodePrefixPF(primaryData, pos)];
        pos += lengthPrefixPF(names.length);

        for (int i=0; i<names.length; i++) {
            int nameLen = decodePrefixPF(primaryData, pos);
            pos += lengthPrefixPF(nameLen);
            String name = decodeStringUTF(primaryData, pos, nameLen).intern();
            pos += nameLen;
            names[i] = name;
            var ci = new ColumnInfo();
            ci.name = name;
            ci.typeCode = decodeIntLE(primaryData, pos); pos += 4;
            info.allColumns.put(name, ci);
        }

        info.keyColumns = new LinkedHashMap<>();
        pos = decodeColumns(primaryData, pos, names, info.keyColumns);

        info.valueColumns = new TreeMap<>();
        pos = decodeColumns(primaryData, pos, names, info.valueColumns);
        if (info.valueColumns.isEmpty()) {
            info.valueColumns = Collections.emptyNavigableMap();
        }

        if (pos < primaryData.length) {
            throw new CorruptDatabaseException
                ("Trailing primary data: " + pos + " < " + primaryData.length);
        }

        if (currentData != null) {
            info.alternateKeys = new TreeSet<>(ColumnSetComparator.THE);
            pos = decodeColumnSets(currentData, 4, names, info.alternateKeys);
            if (info.alternateKeys.isEmpty()) {
                info.alternateKeys = Collections.emptyNavigableSet();
            }

            info.secondaryIndexes = new TreeSet<>(ColumnSetComparator.THE);
            pos = decodeColumnSets(currentData, pos, names, info.secondaryIndexes);
            if (info.secondaryIndexes.isEmpty()) {
                info.secondaryIndexes = Collections.emptyNavigableSet();
            }

            if (pos < currentData.length) {
                throw new CorruptDatabaseException
                    ("Trailing current data: " + pos + " < " + currentData.length);
            }
        }

        return info;
    }

    /**
     * @param columns to be filled in
     * @return updated position
     */
    private static int decodeColumns(byte[] data, int pos, String[] names,
                                     Map<String, ColumnInfo> columns)
    {
        int num = decodePrefixPF(data, pos);
        pos += lengthPrefixPF(num);
        for (int i=0; i<num; i++) {
            int columnNumber = decodePrefixPF(data, pos);
            pos += lengthPrefixPF(columnNumber);
            var ci = new ColumnInfo();
            ci.name = names[columnNumber];
            ci.typeCode = decodeIntLE(data, pos); pos += 4;
            ci.assignType();
            columns.put(ci.name, ci);
        }

        return pos;
    }

    /**
     * @param columnSets to be filled in
     * @return updated position
     */
    private static int decodeColumnSets(byte[] data, int pos, String[] names,
                                        Set<ColumnSet> columnSets)
    {
        int size = decodePrefixPF(data, pos);
        pos += lengthPrefixPF(size);
        for (int i=0; i<size; i++) {
            var cs = new ColumnSet();
            cs.allColumns = new TreeMap<>();
            pos = decodeColumns(data, pos, names, cs.allColumns);
            cs.keyColumns = new LinkedHashMap<>();
            pos = decodeColumns(data, pos, names, cs.keyColumns);
            cs.valueColumns = new TreeMap<>();
            pos = decodeColumns(data, pos, names, cs.valueColumns);
            if (cs.valueColumns.isEmpty()) {
                cs.valueColumns = Collections.emptyNavigableMap();
            }
            columnSets.add(cs);
        }
        return pos;
    }

    private static byte[] key(long indexId) {
        var key = new byte[8];
        encodeLongBE(key, 0, indexId);
        return key;
    }

    private static byte[] key(long indexId, int suffix) {
        var key = new byte[8 + 4];
        encodeLongBE(key, 0, indexId);
        encodeIntBE(key, 8, suffix);
        return key;
    }

    private static class EncodedRowInfo {
        // All the column names, in lexicographical order.
        final String[] names;

        // Primary ColumnSet.
        final byte[] primaryData;

        // Hash code over the primary data.
        final int primaryHash;

        // Current schemaVersion (initially zero), alternateKeys, and secondaryIndexes.
        final byte[] currentData;

        // Set of descriptors.
        final TreeSet<byte[]> secondaries;

        /**
         * Constructor for encoding and writing.
         */
        EncodedRowInfo(RowInfo info) {
            names = new String[info.allColumns.size()];
            var columnNameMap = new HashMap<String, Integer>();
            var encoder = new Encoder(names.length * 16); // with initial capacity guess
            encoder.writeByte(1); // encoding version

            encoder.writePrefixPF(names.length);

            int columnNumber = 0;
            for (ColumnInfo column : info.allColumns.values()) {
                String name = column.name;
                columnNameMap.put(name, columnNumber);
                names[columnNumber++] = name;
                encoder.writeStringUTF(name);
                encoder.writeIntLE(column.typeCode);
            }

            encodeColumns(encoder, info.keyColumns.values(), columnNameMap);
            encodeColumns(encoder, info.valueColumns.values(), columnNameMap);

            primaryData = encoder.toByteArray();
            primaryHash = Arrays.hashCode(primaryData);

            encoder.reset(0);
            encoder.writeIntLE(0); // slot for current schemaVersion
            encodeColumnSets(encoder, info.alternateKeys, columnNameMap);
            encodeColumnSets(encoder, info.secondaryIndexes, columnNameMap);

            currentData = encoder.toByteArray();

            secondaries = new TreeSet<>(Arrays::compareUnsigned);
            info.alternateKeys.forEach(cs -> secondaries.add(encodeDescriptor('A', encoder, cs)));
            info.secondaryIndexes.forEach(cs -> secondaries.add(encodeDescriptor('I', encoder,cs)));
        }

        /**
         * Encode columns using name indexes instead of strings.
         */
        private static void encodeColumns(Encoder encoder,
                                          Collection<ColumnInfo> columns,
                                          Map<String, Integer> columnNameMap)
        {
            encoder.writePrefixPF(columns.size());
            for (ColumnInfo column : columns) {
                encoder.writePrefixPF(columnNameMap.get(column.name));
                encoder.writeIntLE(column.typeCode);
            }
        }

        /**
         * Encode column sets using name indexes instead of strings.
         */
        private static void encodeColumnSets(Encoder encoder,
                                             Collection<ColumnSet> columnSets,
                                             Map<String, Integer> columnNameMap)
        {
            encoder.writePrefixPF(columnSets.size());
            for (ColumnSet cs : columnSets) {
                encodeColumns(encoder, cs.allColumns.values(), columnNameMap);
                encodeColumns(encoder, cs.keyColumns.values(), columnNameMap);
                encodeColumns(encoder, cs.valueColumns.values(), columnNameMap);
            }
        }

        /**
         * Encode a secondary index descriptor.
         *
         * @param type 'A' or 'I'; alternate key descriptors are ordered first
         */
        private static byte[] encodeDescriptor(char type, Encoder encoder, ColumnSet cs) {
            encoder.reset(0);

            encoder.writeByte((byte) type);

            encoder.writePrefixPF(cs.keyColumns.size());
            for (ColumnInfo column : cs.keyColumns.values()) {
                int typeCode = column.typeCode;
                char prefix;
                if (typeCode == -1) {
                    // The "unspecified" prefix is expected only when calling examineIndex from
                    // the indexTable method.
                    prefix = '~';
                } else {
                    prefix = ColumnInfo.isDescending(typeCode) ? '-' : '+';
                }
                encoder.writeByte(prefix);
                encoder.writeStringUTF(column.name);
            }

            encoder.writePrefixPF(cs.valueColumns.size());
            for (ColumnInfo column : cs.valueColumns.values()) {
                encoder.writeStringUTF(column.name);
            }

            return encoder.toByteArray();
        }
    }
}
