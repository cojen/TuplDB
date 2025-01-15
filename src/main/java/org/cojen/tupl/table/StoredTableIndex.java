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

package org.cojen.tupl.table;

import java.io.IOException;

import org.cojen.tupl.Index;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Updater;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnmodifiableViewException;

import org.cojen.tupl.core.RowPredicateLock;

/**
 * Base class for the tables returned by viewAlternateKey and viewSecondaryIndex.
 *
 * @author Brian S O'Neill
 */
public abstract class StoredTableIndex<R> extends StoredTable<R> {
    protected StoredTableIndex(TableManager<R> manager,
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
    public final boolean hasPrimaryKey() {
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
    public final boolean tryInsert(Transaction txn, R row) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public final boolean tryReplace(Transaction txn, R row) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public final boolean tryUpdate(Transaction txn, R row) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public final boolean tryMerge(Transaction txn, R row) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public final boolean tryDelete(Transaction txn, R row) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    protected final StoredTableIndex<R> viewAlternateKey(String... columns) throws IOException {
        throw new IllegalStateException();
    }

    @Override
    protected final StoredTableIndex<R> viewSecondaryIndex(String... columns) throws IOException {
        throw new IllegalStateException();
    }

    @Override
    public Scanner<R> newScanner(R row, Transaction txn, String filter, Object... args)
        throws IOException
    {
        return newScannerThisTable(txn, row, filter, args);
    }

    @Override
    public Updater<R> newUpdater(R row, Transaction txn, String filter, Object... args)
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
                                          StoredTable<R> primaryTable)
        throws IOException
    {
        return primaryTable.newUpdater(row, txn, controller, this);
    }
}
