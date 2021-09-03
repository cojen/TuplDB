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

package org.cojen.tupl.rows;

import java.io.IOException;

import java.lang.ref.WeakReference;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeMap;

import java.util.concurrent.TimeUnit;

import org.cojen.tupl.CorruptDatabaseException;
import org.cojen.tupl.Cursor;
import org.cojen.tupl.Index;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;

import org.cojen.tupl.util.Worker;

import static org.cojen.tupl.rows.RowUtils.*;

/**
 * Generates secondary indexes and alternate keys.
 *
 * @author Brian S O'Neill
 */
class IndexManager<R> {
    private final TreeMap<byte[], WeakReference<SecondaryInfo>> mIndexInfos;

    private TreeMap<byte[], IndexBackfill<R>> mIndexBackfills;

    private WeakReference<Worker> mWorkerRef;

    IndexManager() {
        mIndexInfos = new TreeMap<>(Arrays::compareUnsigned);
    }

    /**
     * Update the set of indexes, based on what is found in the given View. If anything
     * changed, a new trigger is installed on the table. Caller is expected to hold a lock
     * which prevents concurrent calls to this method, which isn't thread-safe.
     *
     * @param rs used to open secondary indexes
     * @param txn holds the lock
     * @param secondaries maps index descriptor to index id and state
     */
    void update(AbstractTable<R> table, RowStore rs,
                Transaction txn, View secondaries, Class<R> rowType)
        throws IOException
    {
        int size = 0;

        hasChanges: {
            try (Cursor c = secondaries.newCursor(txn)) {
                for (c.first(); c.key() != null; c.next()) {
                    if (didSecondaryChange(c)) {
                        // Finish calculating the size and break out.
                        do {
                            size++;
                            c.next();
                        } while (c.key() != null);
                        break hasChanges;
                    }
                    size++;
                }
            }

            if (size == mIndexInfos.size()) {
                // Nothing changed.
                return;
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
        }

        ArrayList<IndexBackfill> newBackfills = null;

        RowInfo primaryInfo = RowInfo.find(rowType);

        var maker = new IndexTriggerMaker<R>(rowType, primaryInfo, size);

        try (Cursor c = secondaries.newCursor(txn)) {
            int i = 0;
            byte[] desc;
            for (c.first(); (desc = c.key()) != null; c.next(), i++) {
                maker.mSecondaryDescriptors[i] = desc;

                WeakReference<SecondaryInfo> infoRef = mIndexInfos.get(desc);
                SecondaryInfo info;
                if (infoRef == null || (info = infoRef.get()) == null) {
                    info = RowStore.indexRowInfo(primaryInfo, desc);
                    mIndexInfos.put(desc, new WeakReference<>(info));
                }
                maker.mSecondaryInfos[i] = info;

                byte[] value = c.value();

                long indexId = decodeLongLE(value, 0);
                Index index = rs.mDatabase.indexById(indexId);
                if (index == null) {
                    throw new CorruptDatabaseException("Secondary index is missing: " + indexId);
                }
                maker.mSecondaryIndexes[i] = index;

                byte state = value[8];
                if (state != 'B') { // not "backfill" state
                    removeIndexBackfill(desc);
                } else {
                    if (mIndexBackfills == null) {
                        mIndexBackfills = new TreeMap<>(Arrays::compareUnsigned);
                    }

                    IndexBackfill<R> backfill = mIndexBackfills.get(desc);

                    if (backfill == null) {
                        backfill = maker.makeBackfill(this, rs, table, i);
                        mIndexBackfills.put(desc, backfill);
                        if (newBackfills == null) {
                            newBackfills = new ArrayList<>();
                        }
                        newBackfills.add(backfill);
                    }

                    maker.mBackfills[i] = backfill;
                }
            }
        }

        if (mIndexBackfills != null && mIndexBackfills.isEmpty()) {
            mIndexBackfills = null;
        }

        table.setTrigger(maker.makeTrigger(rs, table.mSource.id()));

        // Can only safely start new backfills after the new trigger has been installed.

        if (newBackfills != null && !newBackfills.isEmpty()) {
            Worker worker;
            if (mWorkerRef == null || (worker = mWorkerRef.get()) == null) {
                worker = Worker.make(Integer.MAX_VALUE, 1, TimeUnit.SECONDS, null);
                mWorkerRef = new WeakReference<>(worker);
            }
            for (var backfill : newBackfills) {
                worker.enqueue(backfill);
            }
        }
    }

    /**
     * @param c cursor over the secondaries view
     * @return true if a secondary index was added or its state changed
     */
    private boolean didSecondaryChange(Cursor c) {
        byte[] desc = c.key();
        if (!mIndexInfos.containsKey(desc)) {
            return true;
        }

        byte state = c.value()[8];

        IndexBackfill<R> backfill = mIndexBackfills == null ? null : mIndexBackfills.get(desc);

        // If there's no backfill in progress, but the state is "backfill", then return true to
        // start the backfill. If there is (or was) a backfill, but the state isn't "backfill",
        // then return true to stop the backfill or just rebuild the trigger after it finished.
        return (backfill == null) == (state == 'B');
    }

    private void removeIndexBackfill(byte[] desc) {
        if (mIndexBackfills != null) {
            // When an IndexBackfill is removed, it doesn't need to be immediately closed. When
            // the old trigger is disabled, it will call the IndexBackfill.unused method.
            mIndexBackfills.remove(desc);
        }
    }
}
