/*
 *  Copyright (C) 2021 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.table;

import java.io.IOException;

import java.lang.ref.WeakReference;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import org.cojen.tupl.CorruptDatabaseException;
import org.cojen.tupl.Cursor;
import org.cojen.tupl.Index;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;

import org.cojen.tupl.util.Worker;

import static org.cojen.tupl.table.RowUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public final class TableManager<R> {
    final WeakReference<RowStore> mRowStoreRef;
    final Index mPrimaryIndex;

    private final WeakClassCache<StoredTable<R>> mTables;
    private volatile WeakReference<StoredTable<R>> mMostRecentTable;

    private final ConcurrentSkipListMap<byte[], WeakReference<SecondaryInfo>> mIndexInfos;

    private TreeMap<byte[], IndexBackfill<R>> mIndexBackfills;

    private volatile WeakReference<Worker> mWorkerRef;

    private volatile WeakCache<Object, StoredTableIndex<R>, Object> mIndexTables;

    private long mTableVersion;

    TableManager(RowStore rs, Index primaryIndex) {
        mRowStoreRef = rs.ref();
        mPrimaryIndex = primaryIndex;
        mTables = new WeakClassCache<>();
        mIndexInfos = new ConcurrentSkipListMap<>(KEY_COMPARATOR);
    }

    public Index primaryIndex() {
        return mPrimaryIndex;
    }

    /**
     * Is called when the database is closing, to stop any background tasks.
     */
    void shutdown() {
        Worker worker = worker(false);
        if (worker != null) {
            worker.interrupt();
        }
    }

    StoredTable<R> asTable(RowStore rs, Index ix, Class<R> type) throws IOException {
        var table = tryFindTable(type);

        if (table != null) {
            return table;
        }

        table = rs.makeTable(this, ix, type, () -> tryFindTable(type), t -> {
            synchronized (mTables) {
                mMostRecentTable = mTables.put(type, t);
            }
        });

        // Must be called after the table is added to the cache. No harm if called redundantly.
        rs.examineSecondaries(this, true);

        Worker worker = worker(false);
        if (worker != null && ix.isEmpty()) {
            // Backfill of nothing is fast, so wait for it before returning Table to caller.
            worker.join(false);
        }

        return table;
    }

    private StoredTable<R> tryFindTable(Class<R> type) {
        WeakReference<StoredTable<R>> ref = mTables.getRef(type);
        StoredTable<R> table;

        if (ref == null || (table = ref.get()) == null) {
            synchronized (mTables) {
                ref = mTables.getRef(type);
                if (ref == null) {
                    table = null;
                } else if ((table = ref.get()) != null) {
                    mMostRecentTable = ref;
                }
            }
        }

        return table;
    }

    /**
     * Returns a cache of secondary indexes, as tables. See the RowStore.indexTable method.
     */
    WeakCache<Object, StoredTableIndex<R>, Object> indexTables() {
        WeakCache<Object, StoredTableIndex<R>, Object> indexTables = mIndexTables;
        if (indexTables == null) {
            synchronized (this) {
                indexTables = mIndexTables;
                if (indexTables == null) {
                    mIndexTables = indexTables = new WeakCache<>();
                }
            }
        }
        return indexTables;
    }

    synchronized void removeFromIndexTables(long secondaryIndexId) {
        if (mIndexTables != null) {
            if (mIndexTables.removeValues(table -> table.mSource.id() == secondaryIndexId)) {
                mIndexTables = null;
            }
        }
    }

    /**
     * Returns the most recent table that was accessed from the asTable method. If it becomes
     * unreferenced, then this method returns null. By design, there's no linked list of recent
     * tables, as this would create odd race conditions as different tables are GC'd
     * unpredictably.
     */
    StoredTable<R> mostRecentTable() {
        WeakReference<StoredTable<R>> ref = mMostRecentTable;

        if (ref != null) {
            StoredTable<R> table = ref.get();
            if (table != null) {
                return table;
            }
            synchronized (mTables) {
                if (mMostRecentTable == ref) {
                    mMostRecentTable = null;
                }
            }
        }

        return null;
    }

    SecondaryInfo secondaryInfo(RowInfo primaryInfo, byte[] desc) {
        WeakReference<SecondaryInfo> infoRef = mIndexInfos.get(desc);
        SecondaryInfo info;
        if (infoRef == null || (info = infoRef.get()) == null) {
            info = RowStore.secondaryRowInfo(primaryInfo, desc);
            mIndexInfos.put(desc, new WeakReference<>(info));
        }
        return info;
    }

    /**
     * Update the set of indexes, based on what is found in the given View. If anything
     * changed, a new trigger is installed on all tables. Caller is expected to hold a lock
     * which prevents concurrent calls to this method, which isn't thread-safe.
     *
     * <p>IMPORTANT: An optional task object is returned by this method to clear all the
     * necessary query caches, which must be run after the transaction commits. The task isn't
     * expected to be slow, and so it can run in the current thread. Running the task before
     * the transaction commits can lead to deadlock. The deadlock is caused by a combination of
     * the transaction lock and synchronized methods in StoredQueryLauncher.
     *
     * @param tableVersion non-zero current table definition version
     * @param rs used to open secondary indexes
     * @param txn holds the lock
     * @param secondaries maps index descriptor to index id and state
     * @param newTable is true to install a trigger even when the version didn't change;
     * should be true when creating a new Table instance
     * @return an optional task to clear the query caches
     */
    Runnable update(long tableVersion, RowStore rs, Transaction txn, View secondaries,
                    boolean newTable)
        throws IOException
    {
        if (!newTable && tableVersion == mTableVersion) {
            return null;
        }

        Runnable task;

        List<StoredTable<R>> tables = mTables.copyValues();

        if (tables != null) {
            for (var table : tables) {
                Class<R> rowType = table.rowType();
                RowInfo primaryInfo = RowInfo.find(rowType);
                update(table, rowType, primaryInfo, rs, txn, secondaries);
            }
            task = () -> tables.forEach(StoredTable::clearQueryCache);
        } else {
            RowInfo primaryInfo = rs.decodeExisting(txn, null, mPrimaryIndex.id());
            if (primaryInfo != null) {
                update(null, null, primaryInfo, rs, txn, secondaries);
            }
            task = null;
        }

        mTableVersion = tableVersion;

        return task;
    }

    /**
     * @param table can be null if no table instances exist
     * @param rowType can be null if table is null
     * @param primaryInfo required
     * @param secondaries key is secondary index descriptor, value is index id and state
     */
    private void update(StoredTable<R> table, Class<R> rowType, RowInfo primaryInfo,
                        RowStore rs, Transaction txn, View secondaries)
        throws IOException
    {
        int numIndexes = 0;
        try (Cursor c = secondaries.newCursor(txn)) {
            for (c.first(); c.key() != null; c.next()) {
                byte state = c.value()[8];
                if (state != 'D') {
                    numIndexes++;
                }
            }
        }

        // Remove stale entries.
        var it = mIndexInfos.keySet().iterator();
        while (it.hasNext()) {
            byte[] desc = it.next();
            if (!secondaries.exists(txn, desc)) {
                it.remove();
                removeIndexBackfill(desc);
            }
        }

        ArrayList<IndexBackfill> newBackfills = null;

        IndexTriggerMaker<R> maker = null;
        if (numIndexes > 0) {
            maker = new IndexTriggerMaker<>(rowType, primaryInfo, numIndexes);
        }

        try (Cursor c = secondaries.newCursor(txn)) {
            int i = 0;
            byte[] desc;
            for (c.first(); (desc = c.key()) != null; c.next()) {
                byte[] value = c.value();
                long indexId = decodeLongLE(value, 0);
                byte state = value[8];

                if (state != 'B') { // not "backfill" state
                    removeIndexBackfill(desc);

                    if (state == 'D') { // "deleting" state
                        continue;
                    }
                }

                Index index = rs.mDatabase.indexById(indexId);
                if (index == null) {
                    throw new CorruptDatabaseException("Secondary index is missing: " + indexId);
                }

                maker.mSecondaryDescriptors[i] = desc;
                maker.mSecondaryInfos[i] = secondaryInfo(primaryInfo, desc);
                maker.mSecondaryIndexes[i] = index;
                maker.mSecondaryLocks[i] = rs.indexLock(index);

                if (state == 'B') { // "backfill" state
                    if (mIndexBackfills == null) {
                        mIndexBackfills = new TreeMap<>(KEY_COMPARATOR);
                    }

                    IndexBackfill<R> backfill = mIndexBackfills.get(desc);

                    if (backfill == null) {
                        backfill = maker.makeBackfill(rs, mPrimaryIndex.id(), this, i);
                        mIndexBackfills.put(desc, backfill);
                        if (newBackfills == null) {
                            newBackfills = new ArrayList<>();
                        }
                        newBackfills.add(backfill);
                    }

                    maker.mBackfills[i] = backfill;
                }

                i++;
            }
        }

        if (mIndexBackfills != null && mIndexBackfills.isEmpty()) {
            mIndexBackfills = null;
        }

        if (table != null && table.supportsSecondaries()) {
            Trigger<R> trigger = null;
            if (maker != null) {
                trigger = maker.makeTrigger(rs, mPrimaryIndex.id(), table);
            }
            table.setTrigger(trigger);
        }

        // Can only safely start new backfills after the new trigger has been installed.

        if (newBackfills != null && !newBackfills.isEmpty()) {
            Worker worker = worker(true);

            for (var backfill : newBackfills) {
                // The backfill must also be installed as a listener before it starts.
                rs.mDatabase.addRedoListener(backfill);

                worker.enqueue(backfill);
            }
        }
    }

    private void removeIndexBackfill(byte[] desc) {
        if (mIndexBackfills != null) {
            // When an IndexBackfill is removed, it doesn't need to be immediately closed. When
            // the old trigger is disabled, it will call the IndexBackfill.unused method.
            mIndexBackfills.remove(desc);
        }
    }

    /**
     * Returns a Worker instance.
     *
     * @param require when false, null can be returned
     */
    private Worker worker(boolean require) {
        WeakReference<Worker> ref;
        Worker worker;

        if ((ref = mWorkerRef) != null && ((worker = ref.get()) != null)) {
            return worker;
        }

        if (!require) {
            return null;
        }

        synchronized (this) {
            if ((ref = mWorkerRef) != null && ((worker = ref.get()) != null)) {
                return worker;
            }

            worker = Worker.make(Integer.MAX_VALUE, 10, TimeUnit.SECONDS, r -> {
                Thread t = new Thread(r);
                t.setDaemon(true);
                return t;
            });

            mWorkerRef = new WeakReference<>(worker);

            return worker;
        }
    }
}
