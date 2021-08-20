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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.util.Objects;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Index;
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
    protected final Index mSource;

    // MethodHandle signature: RowDecoderEncoder filtered(Object... args)
    private final WeakCache<String, MethodHandle> mFilterFactoryCache;

    private Trigger<R> mTrigger;
    private static final VarHandle cTriggerHandle;

    static {
        try {
            cTriggerHandle = MethodHandles.lookup().findVarHandle
                (AbstractTable.class, "mTrigger", Trigger.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    private final IndexManager<R> mIndexManager;

    /**
     * @param triggers pass true to support triggers
     */
    protected AbstractTable(Index source, boolean triggers) {
        mSource = Objects.requireNonNull(source);
        mFilterFactoryCache = new WeakCache<>();
        if (triggers) {
            Trigger<R> trigger = new Trigger<>();
            trigger.mMode = Trigger.SKIP;
            cTriggerHandle.setRelease(this, trigger);
            mIndexManager = new IndexManager<>();
        } else {
            mIndexManager = null;
        }
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
        AbstractTable<R> table = mTrigger != null ? this : null;

        BasicRowUpdater<R> updater;
        if (txn == null) {
            txn = mSource.newTransaction(null);
            Cursor c = mSource.newCursor(txn);
            try {
                updater = new AutoCommitRowUpdater<>(mSource, c, encoder, table);
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
                updater = new BasicRowUpdater<>(mSource, c, encoder, table);
                break;
            case REPEATABLE_READ:
                updater = new UpgradableRowUpdater<>(mSource, c, encoder, table);
                break;
            case READ_COMMITTED:
            case READ_UNCOMMITTED:
                txn.enter();
                updater = new NonRepeatableRowUpdater<>(mSource, c, encoder, table);
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

    /**
     * Update the secondary indexes, if any. Caller is expected to hold a lock which prevents
     * concurrent calls to this method, which isn't thread-safe.
     *
     * @param rs used to open tables for indexes
     * @param txn holds the lock
     * @param secondaries maps index descriptor to index id and state
     * @throws NullPointerException if unsupported
     */
    void examineSecondaries(RowStore rs, Transaction txn, View secondaries) throws IOException {
        Trigger<R> trigger = mIndexManager.update(rs, txn, secondaries, this);
        if (trigger != null) {
            setTrigger(trigger);
        }
    }

    boolean supportsSecondaries() {
        return mIndexManager != null;
    }

    /**
     * Set the table trigger and then wait for the old trigger to no longer be used. Waiting is
     * necessary to prevent certain race conditions. For example, when adding a secondary
     * index, a backfill task can't safely begin until it's known that no operations are in
     * flight which aren't aware of the new index. Until this method returns, it should be
     * assumed that both the old and new trigger are running concurrently.
     *
     * @param trigger can pass null to remove the trigger
     * @throws UnsupportedOperationException if triggers aren't supported by this table
     */
    void setTrigger(Trigger<R> trigger) {
        if (mTrigger == null) {
            throw new UnsupportedOperationException();
        }

        if (trigger == null) {
            trigger = new Trigger<>();
            trigger.mMode = Trigger.SKIP;
        }

        var old = (Trigger<R>) cTriggerHandle.getAndSet(this, trigger);

        // Note that mode field can be assigned using "plain" mode because lock acquisition
        // applies a volatile fence.
        old.mMode = Trigger.DISABLED;

        // Wait for in-flight operations against the old trigger to finish.
        old.acquireExclusive();
        old.releaseExclusive();

        // At this point, any threads which acquire the shared lock on the trigger will observe
        // that it's disabled by virtue of having applied a volatile fence to obtain the lock
        // in the first place.
    }

    /**
     * Returns the current trigger, which must be held shared during the operation. As soon as
     * acquired, check if the trigger is disabled. This method must be public because it's
     * sometimes accessed from generated code which isn't a subclass of AbstractTable.
     */
    public Trigger<R> trigger() {
        return (Trigger<R>) cTriggerHandle.getOpaque(this);
    }

    static RowFilter parse(Class<?> rowType, String filter) {
        return new Parser(RowInfo.find(rowType).allColumns, filter).parse();
    }
}
