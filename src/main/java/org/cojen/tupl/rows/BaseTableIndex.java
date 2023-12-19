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

import org.cojen.tupl.Index;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Updater;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnmodifiableViewException;

import org.cojen.tupl.core.RowPredicateLock;

import org.cojen.tupl.diag.QueryPlan;

/**
 * Base class for the tables returned by viewAlternateKey and viewSecondaryIndex.
 *
 * @author Brian S O'Neill
 */
public abstract class BaseTableIndex<R> extends BaseTable<R> {
    protected BaseTableIndex(TableManager<R> manager,
                             Index source, RowPredicateLock<R> indexLock)
    {
        super(manager, source, indexLock);
    }

    @Override
    final boolean isEvolvable() {
        return false;
    }

    @Override
    final boolean supportsSecondaries() {
        return false;
    }

    @Override
    public final void store(Transaction txn, R row) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public final R exchange(Transaction txn, R row) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public final void insert(Transaction txn, R row) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public final void replace(Transaction txn, R row) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public final void update(Transaction txn, R row) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public final void merge(Transaction txn, R row) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public final boolean delete(Transaction txn, R row) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    protected final BaseTableIndex<R> viewAlternateKey(String... columns) throws IOException {
        throw new IllegalStateException();
    }

    @Override
    protected final BaseTableIndex<R> viewSecondaryIndex(String... columns) throws IOException {
        throw new IllegalStateException();
    }

    @Override
    protected final Scanner<R> newScanner(R row, Transaction txn, ScanController<R> controller)
        throws IOException
    {
        final BasicScanner<R> scanner;
        final RowPredicateLock.Closer closer;

        if (txn == null) {
            // A null transaction behaves like a read committed transaction (as usual), but it
            // doesn't acquire predicate locks. This makes it weaker than a transaction which
            // is explicitly read committed.

            // Need to guard against this secondary index from being concurrently dropped. This
            // is like adding a predicate lock which matches nothing.
            txn = mSource.newTransaction(null);
            closer = mIndexLock.addGuard(txn);

            if (controller.isJoined()) {
                // Need to retain row locks against the secondary until after the primary
                // row has been loaded.
                txn.lockMode(LockMode.REPEATABLE_READ);
                scanner = new AutoUnlockScanner<>(this, controller);
            } else {
                txn.lockMode(LockMode.READ_COMMITTED);
                scanner = new TxnResetScanner<>(this, controller);
            }
        } else {
            if (txn.lockMode().noReadLock) {
                closer = null;
            } else {
                /*
                  This case is reached when a transaction was provided which is read committed
                  or higher. Adding a predicate lock prevents new rows from being inserted
                  into the scan range for the duration of the transaction scope. If the lock
                  mode is repeatable read, then rows which have been read cannot be deleted,
                  effectively making the transaction serializable.

                  Scans over the primary index only use a predicate lock with UPGRADABLE_READ,
                  but scans over secondaries (like this) need to be more strict because rows
                  can more freely move around the secondary index. The use of a predicate lock
                  prevents the odd case whereby a row is observed multiple times (with updates)
                  during a single scan.

                  Rows in a primary index don't "move" per se, because the primary key cannot
                  be altered. A move is only possible by deleting the old row and inserting a
                  new row. Because this changes the identity of the row, there's no harm in a
                  scan over the primary index seeing the row again, because it's really a
                  different row at this point.
                */
                closer = mIndexLock.addPredicate(txn, controller.predicate());
            }

            scanner = new BasicScanner<>(this, controller);
        }

        try {
            scanner.init(txn, row);
            return scanner;
        } catch (Throwable e) {
            if (closer != null) {
                closer.close();
            }
            throw e;
        }
    }

    @Override
    public Scanner<R> newScanner(R row, Transaction txn, String filter, Object... args)
        throws IOException
    {
        return newScannerThisTable(txn, row, filter, args);
    }

    @Override
    protected Updater<R> newUpdater(R row, Transaction txn, String filter, Object... args)
        throws IOException
    {
        // By default, this will throw an UnmodifiableViewException. See below.
        return newUpdaterThisTable(row, txn, filter, args);
    }

    @Override
    protected Updater<R> newUpdater(R row, Transaction txn, ScanController<R> controller)
        throws IOException
    {
        throw new UnmodifiableViewException();
    }

    protected Updater<R> newJoinedUpdater(R row, Transaction txn,
                                          ScanController<R> controller,
                                          BaseTable<R> primaryTable)
        throws IOException
    {
        return primaryTable.newUpdater(row, txn, controller, this);
    }

    @Override
    public QueryPlan scannerPlan(Transaction txn, String filter, Object... args) {
        return scannerPlanThisTable(txn, filter, args);
    }

    @Override
    public QueryPlan updaterPlan(Transaction txn, String filter, Object... args) {
        return scannerPlan(txn, filter, args);
    }
}
