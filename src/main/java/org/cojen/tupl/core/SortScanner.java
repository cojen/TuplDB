/*
 *  Copyright (C) 2018 Cojen.org
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

package org.cojen.tupl.core;

import java.io.IOException;

import java.lang.invoke.MethodHandle;

import java.util.Comparator;
import java.util.Objects;

import org.cojen.tupl.ClosedIndexException;
import org.cojen.tupl.Entry;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnpositionedCursorException;

/**
 * Scanner implementation intended for scanning and deleting sort results. No other threads
 * should be accessing the source temporary tree, which is deleted when the scan is complete,
 * or when the scanner is closed.
 *
 * @author Brian S O'Neill
 */
class SortScanner implements Scanner<Entry> {
    private static final MethodHandle POPULATOR = EntryPopulator.THE;

    private final LocalDatabase mDatabase;
    private BTreeCursor mCursor;
    private Supplier mSupplier;
    private Entry mEntry;
    private Comparator<Entry> mComparator;

    /**
     * Must call ready or notReady to complete initialization.
     */
    SortScanner(LocalDatabase db) {
        mDatabase = db;
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return ORDERED | DISTINCT | SORTED | NONNULL;
    }

    @Override
    public Comparator<Entry> getComparator() {
        Comparator<Entry> comparator = mComparator;
        if (comparator == null) {
            mComparator = comparator = (a, b) -> Utils.KEY_COMPARATOR.compare(a.key(), b.key());
        }
        return comparator;
    }

    @Override
    public Entry row() {
        Entry e = mEntry;
        if (e == null) {
            if (mCursor == null && tryOpenCursor() == null) {
                return null;
            }
            e = mEntry;
        }
        return e;
    }

    @Override
    public Entry step() throws IOException {
        return doStep((Entry) null);
    }

    @Override
    public Entry step(Entry row) throws IOException {
        Objects.requireNonNull(row);
        return doStep(row);
    }

    private Entry doStep(Entry row) throws IOException {
        BTreeCursor c = mCursor;
        if (c == null && (c = tryOpenCursor()) == null) {
            return null;
        }
        try {
            doStep(c);
            byte[] key = c.key();
            if (key != null) {
                mEntry = row = (Entry) POPULATOR.invokeExact(row, key, c.value());
                return row;
            }
            mCursor = null;
            mEntry = null;
            mDatabase.quickDeleteTemporaryTree(c.mTree);
            return null;
        } catch (UnpositionedCursorException e) {
            mEntry = null;
            return null;
        } catch (Throwable e) {
            throw Utils.fail(this, e);
        }
    }

    protected void doStep(BTreeCursor c) throws IOException {
        c.deleteNext();
    }

    @Override
    public void close() throws IOException {
        try {
            BTreeCursor c = mCursor;
            if (c != null) {
                mCursor = null;
                mEntry = null;
                mDatabase.deleteIndex(c.mTree).run();
            } else if (mSupplier != null) {
                mSupplier.close();
                mSupplier = null;
            }
        } catch (ClosedIndexException e) {
            // Ignore potential double delete.
        } catch (IOException e) {
            if (!mDatabase.isClosed()) {
                throw e;
            }
        }
    }

    void ready(BTree tree) throws IOException {
        var c = new BTreeCursor(tree, Transaction.BOGUS);
        initPosition(c);
        mCursor = c;
        byte[] key = c.key();
        if (key != null) {
            try {
                mEntry = (Entry) POPULATOR.invokeExact((Entry) null, key, c.value());
            } catch (Throwable e) {
                throw Utils.fail(this, e);
            }
        }
    }

    protected void initPosition(BTreeCursor c) throws IOException {
        c.first();
    }

    static interface Supplier {
        BTree get() throws IOException;

        void close() throws IOException;
    }

    void notReady(Supplier supplier) {
        mSupplier = supplier;
    }

    private BTreeCursor tryOpenCursor() {
        try {
            if (mSupplier == null) {
                // Assume that scanner is being used after being closed.
                return null;
            }
            BTree tree = mSupplier.get();
            mSupplier = null;
            ready(tree);
            return mCursor;
        } catch (IOException e) {
            throw Utils.rethrow(e);
        }
    }
}
