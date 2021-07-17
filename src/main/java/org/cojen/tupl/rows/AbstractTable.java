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

import java.lang.invoke.MethodHandle;

import java.util.Objects;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.RowScanner;
import org.cojen.tupl.RowUpdater;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;

import org.cojen.tupl.filter.Parser;
import org.cojen.tupl.filter.RowFilter;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public abstract class AbstractTable<R> implements Table<R> {
    protected final View mSource;

    // MethodHandle signature: RowDecoderEncoder filtered(Object... args)
    private final WeakCache<String, MethodHandle> mFilterFactoryCache;

    protected AbstractTable(View source) {
        mSource = Objects.requireNonNull(source);
        mFilterFactoryCache = new WeakCache<>();
    }

    @Override
    public RowScanner<R> newRowScanner(Transaction txn) throws IOException {
        return newRowScanner(txn, unfiltered());
    }

    @Override
    public RowScanner<R> newRowScanner(Transaction txn, String filter, Object... args)
        throws IOException
    {
        return newRowScanner(txn, filtered(filter, args));
    }

    private RowScanner<R> newRowScanner(Transaction txn, RowDecoderEncoder<R> decoder)
        throws IOException
    {
        var scanner = new BasicRowScanner<>(mSource.newCursor(txn), decoder);
        scanner.init();
        return scanner;
    }

    @Override
    public RowUpdater<R> newRowUpdater(Transaction txn) throws IOException {
        return newRowUpdater(txn, unfiltered());
    }

    @Override
    public RowUpdater<R> newRowUpdater(Transaction txn, String filter, Object... args)
        throws IOException
    {
        return newRowUpdater(txn, filtered(filter, args));
    }

    private RowUpdater<R> newRowUpdater(Transaction txn, RowDecoderEncoder<R> encoder)
        throws IOException
    {
        BasicRowUpdater<R> updater;
        if (txn == null) {
            txn = mSource.newTransaction(null);
            Cursor c = mSource.newCursor(txn);
            try {
                updater = new AutoCommitRowUpdater<>(mSource, c, encoder);
            } catch (Throwable e) {
                try {
                    txn.exit();
                } catch (Throwable e2) {
                    Utils.suppress(e, e2);
                }
                throw e;
            }
        } else {
            Cursor c = mSource.newCursor(txn);
            switch (txn.lockMode()) {
            default:
                updater = new BasicRowUpdater<>(mSource, c, encoder);
                break;
            case REPEATABLE_READ:
                updater = new UpgradableRowUpdater<>(mSource, c, encoder);
                break;
            case READ_COMMITTED:
            case READ_UNCOMMITTED:
                txn.enter();
                txn.lockMode(LockMode.UPGRADABLE_READ);
                updater = new NonRepeatableRowUpdater<>(mSource, c, encoder);
                break;
            }
        }

        updater.init();
        return updater;
    }

    @Override
    public String toString() {
        return mSource.toString();
    }

    @Override
    public Transaction newTransaction(DurabilityMode durabilityMode) {
        return mSource.newTransaction(durabilityMode);
    }

    @Override
    public boolean isEmpty() throws IOException {
        return mSource.isEmpty();
    }

    private RowDecoderEncoder<R> filtered(String filter, Object... args) throws IOException {
        try {
            MethodHandle factory = mFilterFactoryCache.get(filter);
            if (factory == null) {
                factory = findFilterFactory(filter);
            }
            return (RowDecoderEncoder<R>) factory.invokeExact(args);
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    private MethodHandle findFilterFactory(String filter) {
        synchronized (mFilterFactoryCache) {
            MethodHandle factory = mFilterFactoryCache.get(filter);
            if (factory == null) {
                RowFilter rf = parse(rowType(), filter);
                String canonical = rf.toString();
                factory = mFilterFactoryCache.get(canonical);
                if (factory == null) {
                    factory = filteredFactory(canonical, rf);
                    if (!filter.equals(canonical)) {
                        mFilterFactoryCache.put(canonical, factory);
                    }
                }
                mFilterFactoryCache.put(filter, factory);
            }
            return factory;
        }
    }

    /**
     * Returns a singleton instance.
     */
    protected abstract RowDecoderEncoder<R> unfiltered();

    /**
     * Returns a new factory instance, which is cached by the caller.
     *
     * MethodHandle signature: RowDecoderEncoder filtered(Object... args)
     */
    protected abstract MethodHandle filteredFactory(String str, RowFilter filter);

    static RowFilter parse(Class<?> rowType, String filter) {
        return new Parser(RowInfo.find(rowType).allColumns, filter).parse();
    }
}
