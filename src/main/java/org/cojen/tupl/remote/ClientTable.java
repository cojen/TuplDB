/*
 *  Copyright (C) 2022 Cojen.org
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

package org.cojen.tupl.remote;

import java.io.IOException;

import java.util.Comparator;

import java.util.function.Predicate;

import java.util.stream.Stream;

import org.cojen.dirmi.ClosedException;

import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.Updater;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ClientTable<R> implements Table<R> {
    final ClientDatabase mDb;
    final RemoteTable mRemote;
    final Class<R> mType;

    ClientTable(ClientDatabase db, RemoteTable remote, Class<R> type) {
        mDb = db;
        mRemote = remote;
        mType = type;
    }

    @Override
    public Class<R> rowType() {
        return mType;
    }

    @Override
    public R newRow() {
        // FIXME
        throw null;
    }

    @Override
    public R cloneRow(R row) {
        // FIXME
        throw null;
    }

    @Override
    public void unsetRow(R row) {
        // FIXME
        throw null;
    }

    @Override
    public void copyRow(R from, R to) {
        // FIXME
        throw null;
    }

    @Override
    public Scanner<R> newScanner(Transaction txn) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public Scanner<R> newScanner(Transaction txn, String query, Object... args) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public Updater<R> newUpdater(Transaction txn) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public Updater<R> newUpdater(Transaction txn, String query, Object... args) throws IOException {
        // FIXME
        throw null;
    }

    /*
    @Override
    public Stream<R> newStream(Transaction txn) {
        // FIXME: newStream
        throw null;
    }

    @Override
    public Stream<R> newStream(Transaction txn, String query, Object... args) {
        // FIXME: newStream
        throw null;
    }
    */

    @Override
    public Transaction newTransaction(DurabilityMode dm) {
        return ClientTransaction.from(mDb, mRemote.newTransaction(dm), dm);
    }

    @Override
    public boolean isEmpty() throws IOException {
        return mRemote.isEmpty();
    }

    @Override
    public boolean load(Transaction txn, R row) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public boolean exists(Transaction txn, R row) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public void store(Transaction txn, R row) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public R exchange(Transaction txn, R row) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public boolean insert(Transaction txn, R row) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public boolean replace(Transaction txn, R row) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public boolean update(Transaction txn, R row) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public boolean merge(Transaction txn, R row) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public boolean delete(Transaction txn, R row) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public Comparator<R> comparator(String spec) {
        // FIXME
        throw null;
    }

    @Override
    public Predicate<R> predicate(String query, Object... args) {
        // FIXME
        throw null;
    }

    @Override
    public QueryPlan scannerPlan(Transaction txn, String query, Object... args) throws IOException {
        return mRemote.scannerPlan(mDb.remoteTransaction(txn), query, args);
    }

    @Override
    public QueryPlan updaterPlan(Transaction txn, String query, Object... args) throws IOException {
        return mRemote.updaterPlan(mDb.remoteTransaction(txn), query, args);
    }

    @Override
    public QueryPlan streamPlan(Transaction txn, String query, Object... args) throws IOException {
        return mRemote.scannerPlan(mDb.remoteTransaction(txn), query, args);
    }

    @Override
    public void close() throws IOException {
        mRemote.dispose();
    }

    @Override
    public boolean isClosed() {
        try {
            return mRemote.isClosed();
        } catch (Exception e) {
            if (e instanceof ClosedException) {
                return true;
            }
            throw e;
        }
    }
}