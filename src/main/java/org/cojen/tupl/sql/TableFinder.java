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

import java.util.Objects;

import org.cojen.maker.ClassMaker;

import org.cojen.tupl.CorruptDatabaseException;
import org.cojen.tupl.Database;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UniqueConstraintException;

import org.cojen.tupl.core.CoreDatabase;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.table.RowGen;
import org.cojen.tupl.table.RowInfo;
import org.cojen.tupl.table.RowStore;
import org.cojen.tupl.table.WeakCache;

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
    private final Table<TableInfo> mInfoTable;
    private final String mSchema;
    private final ClassLoader mLoader;

    // Maps fully qualified table names to generated row types.
    private final WeakCache<String, Class<?>, Object> mRowTypeCache;

    private TableFinder(Database db, String schema, ClassLoader loader) throws IOException {
        mDb = Objects.requireNonNull(db);
        mInfoTable = mDb.openTable(TableInfo.class);
        mSchema = Objects.requireNonNull(schema);
        if (loader == null) {
            loader = ClassLoader.getSystemClassLoader();
        }
        mLoader = loader;
        mRowTypeCache = new WeakCache<>();
    }

    private TableFinder(TableFinder finder, String schema) {
        mDb = finder.mDb;
        mInfoTable = finder.mInfoTable;
        mSchema = Objects.requireNonNull(schema);
        mLoader = finder.mLoader;
        mRowTypeCache = finder.mRowTypeCache;
    }

    /**
     * @return null if not found
     */
    public Table findTable(String name) throws IOException {
        String fullName = name;
        if (name.indexOf('.') < 0 && !mSchema.isEmpty()) {
            fullName = mSchema + '.' + name;
        }

        String canonicalName = fullName.toLowerCase();

        // First try to find a table or view which was created via an SQL statement.

        Class<?> rowType = mRowTypeCache.get(canonicalName);

        find: {
            Index ix;

            if (rowType != null) {
                ix = mDb.openIndex(canonicalName);
            } else {
                String searchSchema = null, searchName = canonicalName;

                int dotPos = searchName.lastIndexOf('.');
                if (dotPos >= 0) {
                    searchSchema = searchName.substring(0, dotPos);
                    searchName = searchName.substring(dotPos + 1);
                }

                TableInfo infoRow = mInfoTable.newRow();
                infoRow.schema(searchSchema);
                infoRow.name(searchName);

                Transaction txn = mDb.newTransaction();
                try {
                    txn.lockMode(LockMode.REPEATABLE_READ);

                    if (!mInfoTable.load(txn, infoRow)) {
                        break find;
                    }

                    ix = mDb.openIndex(canonicalName);

                    RowStore rs = ((CoreDatabase) mDb).rowStore();
                    RowInfo rowInfo = rs.decodeExisting(txn, canonicalName, ix.id());

                    if (rowInfo == null) {
                        throw new CorruptDatabaseException("Unable to find table definition");
                    }

                    rowType = rowInfo.makeRowType(beginClassMakerForRowType(canonicalName));
                } finally {
                    txn.reset();
                }

                mRowTypeCache.put(canonicalName, rowType);
            }

            return ix.asTable(rowType);
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

        TableInfo infoRow = mInfoTable.newRow();
        infoRow.schema(schema);
        infoRow.name(name);
        infoRow.originalName(fullName.equals(canonicalName) ? null : fullName);
        infoRow.type(TableInfo.TYPE_TABLE);
        infoRow.definition(null);
        infoRow.dependents(null);

        Transaction txn = mDb.newTransaction();
        try {
            try {
                mInfoTable.insert(txn, infoRow);
            } catch (UniqueConstraintException e) {
                if (ifNotExists) {
                    mInfoTable.load(txn, infoRow);
                    if (infoRow.type() == TableInfo.TYPE_VIEW) {
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

        mRowTypeCache.put(canonicalName, rowType);

        return true;
    }

    private static ClassMaker beginClassMakerForRowType(String fullName) {
        return RowGen.beginClassMakerForRowType(TableFinder.class.getPackageName(), fullName);
    }
}
