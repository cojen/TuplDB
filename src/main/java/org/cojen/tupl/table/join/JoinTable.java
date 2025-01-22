/*
 *  Copyright (C) 2023 Cojen.org
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

package org.cojen.tupl.table.join;

import java.io.IOException;

import java.lang.invoke.VarHandle;

import java.lang.ref.SoftReference;

import java.util.Arrays;
import java.util.Comparator;

import java.util.function.Predicate;

import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Query;
import org.cojen.tupl.Row;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.table.BaseTable;
import org.cojen.tupl.table.QueryLauncher;
import org.cojen.tupl.table.RowInfo;
import org.cojen.tupl.table.RowUtils;

import org.cojen.tupl.table.expr.CompiledQuery;

/**
 * Base class for generated join tables.
 *
 * @author Brian S O'Neill
 * @see JoinTableMaker
 */
public abstract class JoinTable<J> extends BaseTable<J> {
    private final String mSpecStr;
    private SoftReference<JoinSpec> mSpecRef;
    private final Table[] mTables;

    /**
     * @param tables table order matches the JoinSpec
     */
    protected JoinTable(String specStr, JoinSpec spec, Table... tables) {
        mSpecStr = specStr;
        mSpecRef = new SoftReference<>(spec);
        mTables = tables;
    }

    /**
     * Constructor to be used by the withTables method.
     */
    protected JoinTable(JoinTable<J> jt, Table... tables) {
        mSpecStr = jt.mSpecStr;
        mSpecRef = jt.mSpecRef;
        mTables = tables;
    }

    @SuppressWarnings("unchecked")
    protected final QueryLauncher<J> scannerQueryLauncher(String query) {
        try {
            return (QueryLauncher<J>) cacheObtain(TYPE_1, query, this);
        } catch (IOException e) {
            // Not expected.
            throw RowUtils.rethrow(e);
        }
    }

    @Override
    public final boolean hasPrimaryKey() {
        return false;
    }

    @Override
    public final Scanner<J> newScanner(J row, Transaction txn) throws IOException {
        return newScanner(row, txn, "{*}", (Object[]) null);
    }

    @Override
    public final Scanner<J> newScanner(J row, Transaction txn, String query, Object... args)
        throws IOException
    {
        return scannerQueryLauncher(query).newScanner(row, txn, args);
    }

    @Override
    public final Transaction newTransaction(DurabilityMode durabilityMode) {
        return mTables[0].newTransaction(durabilityMode);
    }

    @Override
    public final boolean isEmpty() throws IOException {
        return joinSpec().root().isEmpty();
    }

    @Override
    public final Comparator<J> comparator(String spec) {
        return JoinComparatorMaker.comparator(rowType(), spec);
    }

    @Override
    public final Query<J> query(String query) {
        return scannerQueryLauncher(query);
    }

    @Override
    public final boolean tryLoad(Transaction txn, J row) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean exists(Transaction txn, J row) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public final Table<Row> derive(String query, Object... args) throws IOException {
        // See the cacheNewValue method.
        return ((CompiledQuery<Row>) cacheObtain(TYPE_2, query, this)).table(args);
    }

    @Override
    @SuppressWarnings("unchecked")
    public final <D> Table<D> derive(Class<D> derivedType, String query, Object... args)
        throws IOException
    {
        // See the cacheNewValue method.
        var key = new CompiledQuery.DerivedKey(derivedType, query);
        return ((CompiledQuery<D>) cacheObtain(TYPE_2, key, this)).table(args);
    }

    @Override
    public final JoinTable<J> distinct() throws IOException {
        Table[] tables = mTables;
        boolean cloned = false;

        for (int i=0; i<tables.length; i++) {
            Table t = tables[i];
            Table distinct = t.distinct();
            if (t != distinct) {
                if (!cloned) {
                    tables = tables.clone();
                    cloned = true;
                }
                tables[i] = distinct;
            }
        }

        return cloned ? withTables(tables) : this;
    }

    @Override
    public final Predicate<J> predicate(String query, Object... args) {
        return JoinPredicateMaker.newInstance(rowType(), query, args);
    }

    @Override
    public final void close() throws IOException {
        // Do nothing.
    }

    @Override
    public final boolean isClosed() {
        return false;
    }

    @Override
    public final int hashCode() {
        return mSpecStr.hashCode() * 31 + Arrays.hashCode(mTables);
    }

    @Override
    public final boolean equals(Object obj) {
        return obj instanceof JoinTable table
            && mSpecStr.equals(table.mSpecStr) && Arrays.equals(mTables, table.mTables);
    }

    /**
     * Returns tables in the JoinSpec order.
     */
    final Table[] joinTables() {
        return mTables;
    }

    final String joinSpecString() {
        return mSpecStr;
    }

    final JoinSpec joinSpec() {
        JoinSpec spec = mSpecRef.get();

        if (spec == null) {
            spec = JoinSpec.parse(RowInfo.find(rowType()), mSpecStr, mTables);
            var ref = new SoftReference<>(spec);
            VarHandle.storeStoreFence();
            mSpecRef = ref;
        }

        return spec;
    }

    @Override // MultiCache
    protected final Object cacheNewValue(Type type, Object key, Object helper) throws IOException {
        if (type == TYPE_1) { // see the scannerQueryLauncher method
            var queryStr = (String) key;
            return JoinQueryLauncherMaker.newInstance(JoinTable.this, queryStr);
        }

        if (type == TYPE_2) { // see the derive method
            return CompiledQuery.makeDerived(this, type, key, helper);
        }

        throw new AssertionError();
    }

    protected abstract JoinTable<J> withTables(Table[] tables);
}
