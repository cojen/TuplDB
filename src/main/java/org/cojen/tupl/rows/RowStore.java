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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import org.cojen.tupl.View;

import org.cojen.tupl.core.CoreDatabase;

import static org.cojen.tupl.rows.RowUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RowStore {
    private final CoreDatabase mDatabase;

    /* Schema metadata for all types.
     
       (indexId) ->
         int current schemaVersion,
         ColumnSet[] alternateKeys,
         ColumnSet[] secondaryIndexes

       (indexId, schemaVersion) -> primary ColumnSet

       (indexId, hash(primary ColumnSet)) -> schemaVersion[]    // hash collision chain
     
       The schemaVersion is limited to 2^31, and the hash is encoded with bit 31 set,
       preventing collisions. Also, schemaVersion cannot be zero, allowing this form to be
       reserved for future key types.
     */
    private final Index mSchemata;

    private final WeakCache<Pair, AbstractTable<?>> mTableCache;

    public RowStore(CoreDatabase db, Index schemata) throws IOException {
        mDatabase = db;
        mSchemata = schemata;
        mTableCache = new WeakCache<>();
    }

    public Index schemata() {
        return mSchemata;
    }

    @SuppressWarnings("unchecked")
    public <R> Table<R> asTable(Index ix, Class<R> type) throws IOException {
        final var key = new Pair(ix, type);

        AbstractTable rv = mTableCache.get(key);
        if (rv != null) {
            return rv;
        }

        synchronized (mTableCache) {
            rv = mTableCache.get(key);
            if (rv != null) {
                return rv;
            }
        }

        // Throws an exception if type is malformed.
        RowGen gen = RowInfo.find(type).rowGen();

        // Can use NO_FLUSH because transaction will be only used for reading data.
        Transaction txn = mSchemata.newTransaction(DurabilityMode.NO_FLUSH);
        try {
            // With a txn lock held, check if the primary key definition has changed.
            byte[] value = mSchemata.load(txn, key(ix.id()));

            if (value != null) {
                int schemaVersion = decodeIntLE(value, 0);
                String name = type.getName();
                RowInfo currentInfo = decodeExisting(txn, name, ix.id(), value, schemaVersion);
                if (!gen.info.keyColumns.equals(currentInfo.keyColumns)) {
                    throw new IllegalStateException("Cannot alter primary key: " + name);
                }
            }

            synchronized (mTableCache) {
                rv = mTableCache.get(key);
                if (rv != null) {
                    return rv;
                }

                try {
                    var mh = new TableMaker(this, type, gen, ix.id()).finish();
                    rv = (AbstractTable) mh.invoke(ix);
                } catch (Throwable e) {
                    throw rethrow(e);
                }

                mTableCache.put(key, rv);
            }
        } finally {
            txn.reset();
        }

        return rv;
    }

    /**
     * Should be called when the index is being dropped. Does nothing if index wasn't used for
     * storing rows.
     *
     * @param indexKey long index id, big-endian encoded
     */
    public void deleteSchemata(Transaction txn, byte[] indexKey) throws IOException {
        try (Cursor c = mSchemata.viewPrefix(indexKey, 0).newCursor(txn)) {
            c.autoload(false);
            for (c.first(); c.key() != null; c.next()) {
                c.delete();
            }
        }
    }

    /**
     * Returns the schema version for the given row info, creating a new version if necessary.
     */
    int schemaVersion(final RowInfo info, final long indexId) throws IOException {
        int schemaVersion;

        Transaction txn = mSchemata.newTransaction(DurabilityMode.SYNC);
        try (Cursor current = mSchemata.newCursor(txn)) {
            if (mDatabase.isInTrash(txn, indexId)) {
                // Temporary trees are always in the trash, and they don't replicate. For this
                // reason, don't attempt to replicate schema metadata either.
                txn.durabilityMode(DurabilityMode.NO_REDO);
            }

            current.find(key(indexId));

            if (current.value() != null) {
                // Check if the currently defined schema matches.

                schemaVersion = decodeIntLE(current.value(), 0);
                RowInfo currentInfo = decodeExisting
                    (txn, info.name, indexId, current.value(), schemaVersion);

                if (info.matches(currentInfo)) {
                    if (info.alternateKeysMatch(currentInfo) &&
                        info.secondaryIndexesMatch(currentInfo))
                    {
                        // Completely matches.
                        return schemaVersion;
                    }
                    // FIXME: This requires some workflow magic.
                    throw new IllegalStateException("alt keys and indexes don't match");
                }

                if (!info.keyColumns.equals(currentInfo.keyColumns)) {
                    throw new IllegalStateException("Cannot alter primary key: " + info.name);
                }
            }

            final var encoded = new EncodedRowInfo(info);

            // Find an existing schemaVersion or create a new one.

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

                /* FIXME: index set evolution

                   Even when creating a new version, the alt keys and indexes might not
                   match. Something needs to be in RowInfo to track this in all cases.  The
                   default set is whatever is stored currently. Any changes to the definition
                   are tracked as "the new sets". This prevents any immediate issues except
                   when columns are dropped that indexes depend on. Should any changes to
                   alternate keys always be rejected? Removing alt keys is safe, but adding new
                   ones can fail, due to constraints.

                   Names for alternate keys and secondary indexes use '+' and '-' characters,
                   and so they don't conflict with type names. Lookups against these names can
                   be used to determine how far along any workflow proceeded. Final updates to
                   RowInfoStore are made after all new indexes are added and old ones
                   dropped. Actually, adding might require a different order.

                   The '~' character can be used to prefix all the data columns, and once a '~'
                   is used, the '+' and '-' characters won't follow. Given only a name,
                   determining if it refers to an alternate key or secondary index can be
                   deduced by inspection of the key columns. An alternate key always has data
                   columns, but this is optional with a secondary index. A covering index has
                   data columns too. The qualifying distinction is that a secondary index
                   refers to all primary key columns in its own key. An alternate key never
                   refers to all primary key columns in this fashion. At least one primary key
                   column will be in the alternate key data column set. In fact, all of the
                   alternate key data columns must refer to primary key columns.
                */


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

            txn.commit();
        } finally {
            txn.reset();
        }

        return schemaVersion;
    }

    /**
     * Finds a RowInfo for a specific schemaVersion. If not the same as the current version,
     * the alternateKeys and secondaryIndexes will be null (not just empty sets).
     *
     * @return null if not found
     */
    RowInfo rowInfo(Class<?> rowType, long indexId, int schemaVersion) throws IOException {
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
            } else {
                return info;
            }
        } finally {
            txn.reset();
        }
    }

    /**
     * Decodes and caches an existing RowInfo by schemaVersion.
     *
     * @param currentData can be null if not the current schema
     * @return null if not found
     */
    private RowInfo decodeExisting(Transaction txn,
                                   String typeName, long indexId,
                                   byte[] currentData, int schemaVersion)
        throws IOException
    {
        byte[] primaryData = mSchemata.load(txn, key(indexId, schemaVersion));
        return decodeExisting(typeName, currentData, primaryData);
    }

    /**
     * Decodes and caches an existing RowInfo by schemaVersion.
     *
     * @param currentData if null, then alternateKeys and secondaryIndexes won't be decoded
     * @param primaryData if null, then null is returned
     * @return null only if primaryData is null
     */
    private RowInfo decodeExisting(String typeName, byte[] currentData, byte[] primaryData)
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
    private int decodeColumns(byte[] data, int pos, String[] names,
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
    private int decodeColumnSets(byte[] data, int pos, String[] names, Set<ColumnSet> columnSets) {
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
            for (ColumnSet set : columnSets) {
                encodeColumns(encoder, set.allColumns.values(), columnNameMap);
                encodeColumns(encoder, set.keyColumns.values(), columnNameMap);
                encodeColumns(encoder, set.valueColumns.values(), columnNameMap);
            }
        }
    }
}
