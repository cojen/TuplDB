/*
 *  Copyright (C) 2024 Cojen.org
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

package org.cojen.tupl.sql;

import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.cojen.maker.ClassMaker;

import org.cojen.tupl.CorruptDatabaseException;
import org.cojen.tupl.Database;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UniqueConstraintException;

import org.cojen.tupl.core.CoreDatabase;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.OrderBy;
import org.cojen.tupl.table.RowGen;
import org.cojen.tupl.table.RowInfo;
import org.cojen.tupl.table.RowInfoBuilder;
import org.cojen.tupl.table.RowStore;
import org.cojen.tupl.table.WeakCache;

import static org.cojen.tupl.table.RowUtils.encodeStringUTF;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class TableFinder {
    public static TableFinder using(Database db) throws IOException {
        return using(db, "", null);
    }

    /**
     * @param schema base package to use for finding classes; pass an empty string to require
     * fully qualified names
     */
    public static TableFinder using(Database db, String schema) throws IOException {
        return using(db, schema, null);
    }

    /**
     * @param schema base package to use for finding classes; pass an empty to require fully
     * qualified names
     * @param loader used to load table classes
     */
    public static TableFinder using(Database db, String schema, ClassLoader loader)
        throws IOException
    {
        return new TableFinder(db, schema, loader);
    }

    private final Database mDb;
    private final Table<EntityInfo> mEntityTable;
    private final Table<EntityItemInfo> mEntityItemTable;
    private final String mSchema;
    private final ClassLoader mLoader;

    // Maps canonical table names to generated row types.
    private final WeakCache<String, Class<?>, Object> mRowTypeCache;

    // Optional map of canonical table names to alterations to perform against it.
    private Map<String, List<Alteration>> mRowTypeAlterations;

    private TableFinder(Database db, String schema, ClassLoader loader) throws IOException {
        mDb = Objects.requireNonNull(db);
        mEntityTable = mDb.openTable(EntityInfo.class);
        mEntityItemTable = mDb.openTable(EntityItemInfo.class);
        mSchema = Objects.requireNonNull(schema);
        if (loader == null) {
            loader = ClassLoader.getSystemClassLoader();
        }
        mLoader = loader;

        mRowTypeCache = new WeakCache<>() {
            @Override
            protected Class<?> newValue(String canonicalName, Object unused) {
                try {
                    return tryFindRowType(canonicalName);
                } catch (IOException e) {
                    throw Utils.rethrow(e);
                }
            }
        };
    }

    private TableFinder(TableFinder finder, String schema) {
        mDb = finder.mDb;
        mEntityTable = finder.mEntityTable;
        mEntityItemTable = finder.mEntityItemTable;
        mSchema = Objects.requireNonNull(schema);
        mLoader = finder.mLoader;
        mRowTypeCache = finder.mRowTypeCache;
    }

    /**
     * @return null if not found
     */
    public Table findTable(String name) throws IOException {
        String fullName = fullNameFor(name);
        String canonicalName = fullName.toLowerCase();

        // First try to find a table or view which was created via an SQL statement.

        Class<?> rowType = mRowTypeCache.obtain(canonicalName, null);

        if (rowType != null) {
            return mDb.openIndex(canonicalName).asTable(rowType);
        }

        // Try to find a table which is defined by an external interface definition.

        try {
            rowType = mLoader.loadClass(fullName);
        } catch (ClassNotFoundException e) {
            if (name.equals(fullName)) {
                return null;
            }
            try {
                rowType = mLoader.loadClass(name);
            } catch (ClassNotFoundException e2) {
                return null;
            }
        }

        return mDb.findTable(rowType);
    }

    /**
     * @return the base package to use for finding classes; is empty if fully qualified names
     * are required
     */
    public String schema() {
        return mSchema;
    }

    /**
     * @param schema base package to use for finding classes; pass an empty string to require
     * fully qualified names
     */
    public TableFinder withSchema(String schema) {
        return Objects.equals(mSchema, schema) ? this : new TableFinder(this, schema);
    }

    /**
     * @return false if table already exists and ifNotExists is true
     * @throws IllegalStateException if table or view already exists
     */
    boolean createTable(RowInfo rowInfo, boolean ifNotExists) throws IOException {
        String fullName = rowInfo.name;
        String canonicalName = fullName.toLowerCase();

        String schema = null, name = canonicalName;

        {
            int dotPos = name.lastIndexOf('.');
            if (dotPos >= 0) {
                schema = name.substring(0, dotPos);
                name = name.substring(dotPos + 1);
            }
        }

        ClassMaker cm = beginClassMakerForRowType(canonicalName);

        // Don't bother making a class until after basic error checking has completed.
        Class<?> rowType;

        EntityInfo entity = mEntityTable.newRow();
        entity.schema(schema);
        entity.name(name);
        entity.originalName(fullName.equals(canonicalName) ? null : fullName);
        entity.type(EntityInfo.TYPE_TABLE);
        entity.definition(null);
        entity.dependents(null);

        Transaction txn = mDb.newTransaction();
        try {
            txn.durabilityMode(DurabilityMode.SYNC);

            try {
                mEntityTable.insert(txn, entity);
            } catch (UniqueConstraintException e) {
                if (ifNotExists) {
                    mEntityTable.load(txn, entity);
                    if (entity.type() == EntityInfo.TYPE_VIEW) {
                        throw new IllegalStateException
                            ("Name conflict with an existing view: " + fullName);
                    }
                    return false;
                }
                throw new IllegalStateException("Table or view already exists: " + fullName);
            }

            // This check doesn't prevent race conditions, nor does it prevent the underlying
            // core index from being clobbered later. It can prevent simple mistakes, however.
            if (mDb.findIndex(canonicalName) != null) {
                throw new IllegalStateException("Name conflict with a core index" + fullName);
            }

            // Additional column information might need to be stored.
            for (ColumnInfo ci : rowInfo.allColumns.values()) {
                if (!ci.isHidden() && !ci.isAutomatic()) {
                    continue;
                }

                EntityItemInfo item = mEntityItemTable.newRow();
                item.entitySchema(schema);
                item.entityName(name);
                item.type(EntityItemInfo.TYPE_COLUMN);
                String lowerName = ci.name.toLowerCase();
                item.name(lowerName);
                item.originalName(ci.name.equals(lowerName) ? null : ci.name);

                var definition = new byte[1 + 1 + 8 + 8];
                definition[0] = 0; // encoding version
                definition[1] = ci.isHidden() ? (byte) 1 : (byte) 0;
                Utils.encodeLongLE(definition, 2, ci.autoMin);
                Utils.encodeLongLE(definition, 2 + 8, ci.autoMax);
                item.definition(definition);

                mEntityItemTable.insert(txn, item);
            }

            rowType = rowInfo.makeRowType(cm);

            Index ix = mDb.openIndex(canonicalName);

            try {
                ix.asTable(rowType);
            } catch (Throwable e) {
                // Something is wrong with the table definition. Because the call to openIndex
                // isn't transactional, it cannot be rolled back. Attempt to drop the index
                // instead, which will fail if it's not empty.
                try {
                    ix.drop();
                } catch (Throwable e2) {
                    Utils.suppress(e, e2);
                }

                throw e;
            }

            txn.commit();
        } finally {
            txn.reset();
        }

        synchronized (mRowTypeCache) {
            if (mRowTypeCache.get(canonicalName) == null) {
                mRowTypeCache.put(canonicalName, rowType);
            }
        }

        return true;
    }

    /**
     * @param spec index spec using OrderBy format
     * @return false if index already exists and ifNotExists is true
     * @throws IllegalStateException if index already exists
     */
    boolean createIndex(String tableName, String indexName, String spec,
                        boolean altKey, boolean ifNotExists)
        throws IOException
    {
        String canonicalName = fullNameFor(tableName).toLowerCase();

        var alteration = new Alteration(tableName) {
            boolean result = true;

            @Override
            RowInfo doApply(Transaction txn, EntityInfo entity, RowInfo rowInfo)
                throws IOException
            {
                EntityItemInfo item = mEntityItemTable.newRow();
                item.entitySchema(entity.schema());
                item.entityName(entity.name());
                item.type(EntityItemInfo.TYPE_INDEX);
                String lowerName = indexName.toLowerCase();
                item.name(lowerName);
                item.originalName(indexName.equals(lowerName) ? null : indexName);
                item.definition(encodeStringUTF(((altKey ? 'A' : 'S') + spec)));

                try {
                    mEntityItemTable.insert(txn, item);
                } catch (UniqueConstraintException e) {
                    if (ifNotExists) {
                        result = false;
                        return rowInfo;
                    }
                    throw new IllegalStateException
                        ("An index with the same name already exists: " + indexName);
                }

                var b = new RowInfoBuilder(rowInfo.name);
                b.addAll(rowInfo);

                // FIXME: Move all the OrderBy and finish stuff to the builder. It can later
                // also support reading the spec for dropping indexes too.

                for (OrderBy.Rule rule : OrderBy.forSpec(rowInfo, spec).values()) {
                    if (altKey) {
                        b.addToAlternateKey(rule.asIndexElement());
                    } else {
                        b.addToSecondaryIndex(rule.asIndexElement());
                    }
                }

                if (altKey) {
                    b.finishAlternateKey();
                } else {
                    b.finishSecondaryIndex();
                }

                rowInfo = b.build();

                return rowInfo;
            }
        };

        Class<?> rowType = applyAlteration(canonicalName, alteration);

        if (!alteration.result) {
            return false;
        }

        // As a side-effect, this defines the index and starts building it in the background.
        mDb.openIndex(canonicalName).asTable(rowType);

        // FIXME: wait for index build to finish

        return true;
    }

    private String fullNameFor(String name) {
        String fullName = name;
        if (name.indexOf('.') < 0 && !mSchema.isEmpty()) {
            fullName = mSchema + '.' + name;
        }
        return fullName;
    }

    /**
     * Apply an alteration to a table, forcing a new RowType interface to be defined.
     */
    private Class<?> applyAlteration(String canonicalName, Alteration alteration)
        throws IOException
    {
        // FIXME: Perform a quick check on the alteration to see if it likely will do anything
        // at all. If not, then this prevents throwing away the row type unnecessarily.

        synchronized (mRowTypeCache) {
            List<Alteration> list;
            if (mRowTypeAlterations == null) {
                mRowTypeAlterations = new HashMap<>();
                list = null;
            } else {
                list = mRowTypeAlterations.get(canonicalName);
            }
            if (list == null) {
                list = new ArrayList<>();
                mRowTypeAlterations.put(canonicalName, list);
            }
            list.add(alteration);
            mRowTypeCache.removeKey(canonicalName);
        }

        Class<?> rowType = mRowTypeCache.obtain(canonicalName, null);

        alteration.check();

        return rowType;
    }

    /**
     * Don't call directly. Use mRowTypeCache.obtain instead.
     *
     * @param canonicalName full table name, lowercase
     * @return null if not found
     */
    private Class<?> tryFindRowType(String canonicalName) throws IOException {
        String searchSchema = null, searchName = canonicalName;

        int dotPos = searchName.lastIndexOf('.');
        if (dotPos >= 0) {
            searchSchema = searchName.substring(0, dotPos);
            searchName = searchName.substring(dotPos + 1);
        }

        EntityInfo entity = mEntityTable.newRow();
        entity.schema(searchSchema);
        entity.name(searchName);

        RowInfo rowInfo = null;

        List<Alteration> alterations = null;

        synchronized (mRowTypeCache) {
            if (mRowTypeAlterations != null) {
                alterations = mRowTypeAlterations.remove(canonicalName);
                if (mRowTypeAlterations.isEmpty()) {
                    mRowTypeAlterations = null;
                }
            }
        }

        Transaction txn = mDb.newTransaction();
        try {
            txn.lockMode(LockMode.REPEATABLE_READ);
            txn.durabilityMode(DurabilityMode.SYNC);

            if (!mEntityTable.tryLoad(txn, entity)) {
                if (alterations != null) {
                    for (Alteration alt : alterations) {
                        alt.failed(null, null);
                    }
                }
                return null;
            }

            Index ix = mDb.openIndex(canonicalName);

            RowStore rs = ((CoreDatabase) mDb).rowStore();
            rowInfo = rs.decodeExisting(txn, canonicalName, ix.id());

            if (rowInfo == null) {
                throw new CorruptDatabaseException("Unable to find table definition");
            }

            // Retrieve extra column info.
            try (Scanner<EntityItemInfo> s = mEntityItemTable.newScanner
                 (txn, "entitySchema == ? && entityName == ? && type == ?",
                  searchSchema, searchName, EntityItemInfo.TYPE_COLUMN))
            {
                for (EntityItemInfo item = s.row(); item != null; item = s.step(item)) {
                    ColumnInfo ci = rowInfo.allColumns.get(item.name());
                    if (ci == null) {
                        // Unknown column; not expected.
                        continue;
                    }
                    byte[] definition = item.definition();
                    if (definition[0] != 0) {
                        // Unknown encoding; not expected.
                        continue;
                    }
                    ci.hidden = definition[1] == 1;
                    ci.autoMin = Utils.decodeLongBE(definition, 2);
                    ci.autoMax = Utils.decodeLongBE(definition, 2 + 8);
                }
            }

            if (alterations != null) {
                for (Alteration alt : alterations) {
                    txn.enter();
                    try {
                        rowInfo = alt.apply(txn, entity, rowInfo);
                        txn.commit();
                    } finally {
                        txn.exit();
                    }
                }
            }

            txn.commit();
        } catch (Throwable e) {
            if (alterations != null) {
                for (Alteration alt : alterations) {
                    alt.failed(e, rowInfo);
                }
            }
            throw e;
        } finally {
            txn.reset();
        }

        return rowInfo.makeRowType(beginClassMakerForRowType(canonicalName));
    }

    private static ClassMaker beginClassMakerForRowType(String fullName) {
        return RowGen.beginClassMakerForRowType(TableFinder.class.getPackageName(), fullName);
    }

    private abstract class Alteration {
        protected final String mTableName;

        private volatile Object mFinished;

        /**
         * @param tableName original table name which was requested to be altered
         */
        Alteration(String tableName) {
            mTableName = tableName;
        }

        /**
         * Called when tryFindRowType threw an exception or couldn't find the RowInfo. If the
         * given exception is null, then the RowInfo must also be null.
         *
         * @param e is null if no exception
         * @param rowInfo is null if not found
         */
        final void failed(Throwable e, RowInfo rowInfo) {
            mFinished = e == null ? false : e;
        }

        /**
         * The implementation is expected to modify the given RowInfo object or return a new
         * instance. It can also persist changes using the transaction.
         */
        final RowInfo apply(Transaction txn, EntityInfo entity, RowInfo rowInfo) {
            try {
                rowInfo = doApply(txn, entity, rowInfo);
                mFinished = true;
            } catch (Throwable e) {
                mFinished = e;
            }
            return rowInfo;
        }

        final void check() throws IOException {
            Object finished = mFinished;

            if (finished == Boolean.TRUE) {
                return;
            }

            if (finished == Boolean.FALSE) {
                throw new IllegalStateException("Table isn't found: " + mTableName);
            }

            if (finished instanceof Throwable e) {
                throw Utils.rethrow(e);
            }

            throw new AssertionError();
        }

        abstract RowInfo doApply(Transaction txn, EntityInfo entity, RowInfo rowInfo)
            throws IOException;
    }
}
