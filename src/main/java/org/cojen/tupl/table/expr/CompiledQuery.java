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

package org.cojen.tupl.table.expr;

import java.io.IOException;

import java.util.stream.Stream;

import org.cojen.tupl.Row;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.Updater;

import org.cojen.tupl.core.TupleKey;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.table.MultiCache;
import org.cojen.tupl.table.QueryLauncher;
import org.cojen.tupl.table.RowUtils;
import org.cojen.tupl.table.RowWriter;

/**
 * @author Brian S. O'Neill
 */
public abstract class CompiledQuery<R> extends QueryLauncher<R> {
    /**
     * Returns the query as a fully functional table.
     *
     * @throws IllegalArgumentException if not enough arguments are given
     */
    public abstract Table<R> table(Object... args) throws IOException;

    /**
     * @hidden
     */
    public Table<R> table() throws IOException {
        return table(RowUtils.NO_ARGS);
    }

    @Override
    public Scanner<R> newScanner(R row, Transaction txn, Object... args) throws IOException {
        return table(args).newScanner(row, txn);
    }

    /**
     * @param row initial row; can be null
     */
    @Override
    public Updater<R> newUpdater(R row, Transaction txn, Object... args) throws IOException {
        return table(args).newUpdater(row, txn);
    }

    @Override
    public Stream<R> newStream(Transaction txn, Object... args) {
        try {
            return table(args).newStream(txn);
        } catch (IOException e) {
            throw RowUtils.rethrow(e);
        }
    }

    @Override
    public long deleteAll(Transaction txn, Object... args) throws IOException {
        return table(args).queryAll().deleteAll(txn);
    }

    @Override
    public boolean anyRows(Transaction txn, Object... args) throws IOException {
        return table(args).queryAll().anyRows(txn);
    }

    @Override
    public void scanWrite(Transaction txn, RowWriter writer, Object... args) throws IOException {
        // FIXME: scanWrite
        throw null;
    }

    @Override
    public QueryPlan scannerPlan(Transaction txn, Object... args) throws IOException {
        return table(args).queryAll().scannerPlan(txn);
    }

    @Override
    public QueryPlan updaterPlan(Transaction txn, Object... args) throws IOException {
        return table(args).queryAll().updaterPlan(txn);
    }

    @Override
    public QueryPlan streamPlan(Transaction txn, Object... args) throws IOException {
        return table(args).queryAll().streamPlan(txn);
    }

    @Override
    protected void clearCache() {
        // Nothing to do.
    }

    /**
     * Returns an instance which just wraps a Table.
     */
    public static <R> CompiledQuery<R> make(Table<R> table) {
        return new CompiledQuery<R>() {
            @Override
            public Class<R> rowType() {
                return table.rowType();
            }

            @Override
            public int argumentCount() {
                return 0;
            }

            @Override
            public Table<R> table(Object... args) {
                return table;
            }

            @Override
            protected void closeIndexes() throws IOException {
                table.close();
            }
        };
    }

    /**
     * Make a new or cached CompiledQuery instance, suitable for queries which derive new
     * columns.
     *
     * @param cache stores canonical CompiledQuery instances
     * @param type type to be passed to the cache instance
     * @param key must be a query string
     * @param helper must be a Table instance
     */
    @SuppressWarnings("unchecked")
    public static CompiledQuery<Row> makeDerived
        (MultiCache<? super TupleKey,? super CompiledQuery,? super RelationExpr,IOException> cache,
         MultiCache.Type type, Object key, Object helper)
        throws IOException
    {
        if (key instanceof TupleKey) {
            return ((RelationExpr) helper).makeCompiledRowQuery();
        }
        var queryStr = (String) key;
        RelationExpr expr = Parser.parse((Table) helper, null, queryStr);
        // Obtain the canonical instance and map to that.
        TupleKey canonicalKey = expr.makeKey();
        return (CompiledQuery<Row>) cache.cacheObtain(type, canonicalKey, expr);
    }

    public static abstract class Wrapped<R> extends CompiledQuery<R> {
        protected final CompiledQuery<R> source;
        protected final int argCount;

        protected Wrapped(CompiledQuery<R> source, int argCount) {
            this.source = source;
            this.argCount = argCount;
        }

        @Override
        public Class<R> rowType() {
            return source.rowType();
        }

        @Override
        public final int argumentCount() {
            return argCount;
        }

        protected final int checkArgumentCount(Object... args) {
            int argCount = this.argCount;
            if (args.length < argCount) {
                throw new IllegalArgumentException("Not enough query arguments provided");
            }
            return argCount;
        }

        @Override
        protected void closeIndexes() throws IOException {
            source.closeIndexes();
        }
    }
}
