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
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.table.QueryLauncher;
import org.cojen.tupl.table.RowInfo;
import org.cojen.tupl.table.SoftCache;

/**
 * Base class for generated join tables.
 *
 * @author Brian S O'Neill
 * @see JoinTableMaker
 */
public abstract class JoinTable<J> implements Table<J> {
    private final String mSpecStr;
    private SoftReference<JoinSpec> mSpecRef;
    private final Table[] mTables;

    private final SoftCache<String, QueryLauncher<J>, Object> mQueryLauncherCache;

    /**
     * @param tables table order matches the JoinSpec
     */
    protected JoinTable(String specStr, JoinSpec spec, Table... tables) {
        mSpecStr = specStr;
        mSpecRef = new SoftReference<>(spec);
        mTables = tables;

        mQueryLauncherCache = new SoftCache<>() {
            @Override
            protected QueryLauncher<J> newValue(String query, Object unused) {
                return JoinQueryLauncherMaker.newInstance(JoinTable.this, query);
            }
        };
    }

    protected final QueryLauncher<J> scannerQueryLauncher(String query) {
        return mQueryLauncherCache.obtain(query, null);
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
    public Transaction newTransaction(DurabilityMode durabilityMode) {
        return mTables[0].newTransaction(durabilityMode);
    }

    @Override
    public boolean isEmpty() throws IOException {
        return joinSpec().root().isEmpty();
    }

    @Override
    public final Comparator<J> comparator(String spec) {
        return JoinComparatorMaker.comparator(rowType(), spec);
    }

    @Override
    public Query<J> query(String query) throws IOException {
        return scannerQueryLauncher(query);
    }

    @Override
    public boolean load(Transaction txn, J row) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean exists(Transaction txn, J row) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Predicate<J> predicate(String query, Object... args) {
        return JoinPredicateMaker.newInstance(rowType(), query, args);
    }

    @Override
    public void close() throws IOException {
        // Do nothing.
    }

    @Override
    public boolean isClosed() {
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
    Table[] joinTables() {
        return mTables;
    }

    String joinSpecString() {
        return mSpecStr;
    }

    JoinSpec joinSpec() {
        JoinSpec spec = mSpecRef.get();

        if (spec == null) {
            spec = JoinSpec.parse(RowInfo.find(rowType()), mSpecStr, mTables);
            var ref = new SoftReference<>(spec);
            VarHandle.storeStoreFence();
            mSpecRef = ref;
        }

        return spec;
    }
}
