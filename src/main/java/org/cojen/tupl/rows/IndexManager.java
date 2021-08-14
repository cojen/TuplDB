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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.TreeMap;

import org.cojen.tupl.CorruptDatabaseException;
import org.cojen.tupl.Cursor;
import org.cojen.tupl.Index;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;

import static org.cojen.tupl.rows.RowUtils.*;

/**
 * Generates secondary indexes and alternate keys.
 *
 * @author Brian S O'Neill
 */
class IndexManager<R> {
    private final TreeMap<byte[], WeakReference<RowInfo>> mIndexInfos;

    IndexManager() {
        mIndexInfos = new TreeMap<>(Arrays::compareUnsigned);
    }

    /**
     * Update the set of indexes, based on what is found in the given View. If nothing changed,
     * null is returned. Otherwise, a new Trigger is returned which replaces the existing
     * primary table trigger. Caller is expected to hold a lock which prevents concurrent calls
     * to this method, which isn't thread-safe.
     *
     * @param rs used to open tables for indexes
     * @param txn holds the lock
     * @param secondaries maps index descriptor to index id and state
     * @param table owns the secondaries
     */
    Trigger<R> update(RowStore rs, Transaction txn, View secondaries, AbstractTable<R> table)
        throws IOException
    {
        int size = 0;

        hasChanges: {
            try (Cursor c = secondaries.newCursor(txn)) {
                c.autoload(false);
                for (c.first(); c.key() != null; c.next()) {
                    if (!mIndexInfos.containsKey(c.key())) {
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
                return null;
            }

            // Remove stale entries.
            var it = mIndexInfos.keySet().iterator();
            while (it.hasNext()) {
                if (!secondaries.exists(txn, it.next())) {
                    it.remove();
                }
            }
        }

        RowInfo primaryInfo = RowInfo.find(table.rowType());

        var secondaryInfos = new RowInfo[size];
        var secondaryIndexes = new Index[size];
        var secondaryStates = new byte[size];

        try (Cursor c = secondaries.newCursor(txn)) {
            int i = 0;
            byte[] desc;
            for (c.first(); (desc = c.key()) != null; c.next(), i++) {
                WeakReference<RowInfo> infoRef = mIndexInfos.get(desc);
                RowInfo info;
                if (infoRef == null || (info = infoRef.get()) == null) {
                    info = buildIndexRowInfo(primaryInfo, desc);
                    mIndexInfos.put(desc, new WeakReference<>(info));
                }
                secondaryInfos[i] = info;

                byte[] value = c.value();

                long indexId = decodeLongLE(value, 0);
                Index index = rs.mDatabase.indexById(indexId);
                if (index == null) {
                    throw new CorruptDatabaseException("Secondary index is missing: " + indexId);
                }
                secondaryIndexes[i] = index;

                secondaryStates[i] = value[8];
            }
        }

        // FIXME
        return null;
    }

    private static RowInfo buildIndexRowInfo(RowInfo primaryInfo, byte[] desc) {
        var info = new RowInfo(null);

        // Descriptor is encoded by RowStore.EncodedRowInfo.encodeDescriptor.

        int numKeys = decodePrefixPF(desc, 0);
        int offset = lengthPrefixPF(numKeys);
        info.keyColumns = new LinkedHashMap<>(numKeys);

        for (int i=0; i<numKeys; i++) {
            boolean descending = desc[offset++] == '-';
            int nameLength = decodePrefixPF(desc, offset);
            offset += lengthPrefixPF(nameLength);
            String name = decodeStringUTF(desc, offset, nameLength);
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

            for (int i=0; i<numKeys; i++) {
                int nameLength = decodePrefixPF(desc, offset);
                offset += lengthPrefixPF(nameLength);
                String name = decodeStringUTF(desc, offset, nameLength);
                offset += nameLength;

                ColumnInfo column = primaryInfo.allColumns.get(name);

                info.valueColumns.put(name, column);
            }
        }

        info.allColumns = new TreeMap<>(info.keyColumns);
        info.allColumns.putAll(info.valueColumns);

        return info;
    }
}
