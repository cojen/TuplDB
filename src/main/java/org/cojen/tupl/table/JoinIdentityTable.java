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

package org.cojen.tupl.table;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.io.IOException;

import java.util.Collections;
import java.util.Comparator;

import java.util.stream.Stream;

import org.cojen.tupl.ColumnProcessor;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Query;
import org.cojen.tupl.Row;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnmodifiableViewException;
import org.cojen.tupl.Updater;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.table.expr.Cardinality;
import org.cojen.tupl.table.expr.CompiledQuery;
import org.cojen.tupl.table.expr.Parser;

/**
 * Defines an unmodifiable table consisting of one row with no columns. It represents the
 * identity element when joining tables. Joining tables multiplies the number of rows, and so a
 * join of zero tables should produce one row. This table is useful for representing queries of
 * the form "select 1", which is replaced with "select 1 from identity".
 *
 * @author Brian S. O'Neill
 */
public final class JoinIdentityTable extends BaseTable<Row> implements Query<Row> {
    // Singleton instance.
    public static final JoinIdentityTable THE = new JoinIdentityTable();

    private static volatile EmptyQuery<Row> cEmptyQuery;

    private static final MethodHandle NEW_ROW;

    static {
        try {
            Class<? extends Row> rowClass = RowMaker.find(Row.class);
            NEW_ROW = MethodHandles.lookup().findConstructor
                (rowClass, MethodType.methodType(void.class))
                .asType(MethodType.methodType(Row.class));
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    private JoinIdentityTable() {
    }

    @Override
    public boolean hasPrimaryKey() {
        return false;
    }

    @Override
    public Class<Row> rowType() {
        return Row.class;
    }

    @Override
    public int argumentCount() {
        return 0;
    }

    @Override
    public Row newRow() {
        try {
            return (Row) NEW_ROW.invokeExact();
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    @Override
    public Row cloneRow(Row row) {
        return newRow();
    }

    @Override
    public void unsetRow(Row row) {
        // Nothing to do.
    }

    @Override
    public void cleanRow(Row row) {
        // Nothing to do.
    }

    @Override
    public void copyRow(Row from, Row to) {
        // Nothing to do.
    }

    @Override
    public boolean isSet(Row row, String name) {
        throw new IllegalArgumentException();
    }

    @Override
    public void forEach(Row row, ColumnProcessor<? super Row> action) {
        // Nothing to do.
    }

    @Override
    public Scanner<Row> newScanner(Transaction txn) {
        return new ScanOne();
    }

    @Override
    public Scanner<Row> newScanner(Row row, Transaction txn) {
        return new ScanOne(row);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Query<Row> query(String query) throws IOException {
        return (Query<Row>) cacheObtain(TYPE_1, query, this);
    }

    @Override
    public boolean anyRows(Transaction txn) {
        return true;
    }

    @Override
    public boolean anyRows(Row row, Transaction txn) {
        return true;
    }

    @Override
    public Transaction newTransaction(DurabilityMode durabilityMode) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean tryLoad(Transaction txn, Row row) {
        return true;
    }

    @Override
    public boolean exists(Transaction txn, Row row) {
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Table<Row> derive(String query, Object... args) throws IOException {
        // See the cacheNewValue method.
        return ((CompiledQuery<Row>) cacheObtain(TYPE_2, query, this)).table(args);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <D> Table<D> derive(Class<D> derivedType, String query, Object... args)
        throws IOException
    {
        // See the cacheNewValue method.
        var key = new CompiledQuery.DerivedKey(derivedType, query);
        return ((CompiledQuery<D>) cacheObtain(TYPE_2, key, this)).table(args);
    }

    @Override
    public Table<Row> distinct() {
        return this;
    }

    @Override
    public Comparator<Row> comparator(String spec) {
        // Validate.
        OrderBy.forSpec(Collections.emptyMap(), spec);
        return comparator();
    }

    @SuppressWarnings("unchecked")
    private static Comparator<Row> comparator() {
        return ComparatorMaker.zero();
    }

    @Override
    public void close() {
        // Do nothing.
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    // Implement additional methods defined in the Query interface.

    @Override
    public Scanner<Row> newScanner(Transaction txn, Object... args) {
        return new ScanOne();
    }

    @Override
    public Scanner<Row> newScanner(Row row, Transaction txn, Object... args) {
        return new ScanOne(row);
    }

    @Override
    public Updater<Row> newUpdater(Transaction txn) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public Updater<Row> newUpdater(Row row, Transaction txn) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public Stream<Row> newStream(Transaction txn) {
        return newStream(txn, RowUtils.NO_ARGS);
    }

    @Override
    public boolean anyRows(Transaction txn, Object... args) {
        return true;
    }

    @Override
    public boolean anyRows(Row row, Transaction txn, Object... args) {
        return true;
    }

    @Override
    public QueryPlan scannerPlan(Transaction txn, Object... args) {
        return new QueryPlan.Identity();
    }

    @Override // MultiCache
    protected Object cacheNewValue(Type type, Object key, Object helper) throws IOException {
        if (type == TYPE_1) { // see the query method
            var queryStr = (String) key;
            if (Parser.parse(queryStr).type().cardinality() != Cardinality.ZERO) {
                return this;
            }
            EmptyQuery<Row> empty = cEmptyQuery;
            if (empty == null) {
                cEmptyQuery = empty = new EmptyQuery<>(Row.class);
            }
            return empty;
        }

        if (type == TYPE_2) { // see the derive method
            return CompiledQuery.makeDerived(this, type, key, helper);
        }

        throw new AssertionError();
    }

    private static final class ScanOne implements Scanner<Row> {
        private static final Row ROW = THE.newRow();

        private Row mRow;

        ScanOne() {
            this(ROW);
        }

        ScanOne(Row row) {
            mRow = row == null ? ROW : row;
        }

        @Override
        public Row row() {
            return mRow;
        }

        @Override
        public Row step(Row row) {
            mRow = null;
            return null;
        }

        @Override
        public void close() {
            mRow = null;
        }

        @Override
        public long estimateSize() {
            return 1;
        }

        @Override
        public int characteristics() {
            return ORDERED | DISTINCT | SORTED | SIZED | NONNULL | IMMUTABLE;
        }

        @Override
        public Comparator<? super Row> getComparator() {
            return comparator();
        }
    }
}
