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

package org.cojen.tupl.core;

import java.io.File;
import java.io.IOException;

import java.util.Arrays;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Index;
import org.cojen.tupl.Transaction;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class Rebuilder {
    private final Launcher mOldLauncher, mNewLauncher;
    private final int mNumThreads;

    private LocalDatabase mOldDb, mNewDb;

    public Rebuilder(Launcher oldLauncher, Launcher newLauncher, int numThreads) {
        mOldLauncher = oldLauncher.clone();
        mNewLauncher = newLauncher.clone();
        mNumThreads = numThreads;
    }

    public LocalDatabase run() throws IOException {
        try {
            doRun();
            mNewDb.checkpoint();
        } catch (Throwable e) {
            Utils.closeQuietly(mNewDb);
            throw e;
        } finally {
            Utils.closeQuietly(mOldDb);
        }

        return mNewDb;
    }

    private void doRun() throws IOException {
        // Prepare configuration and perform basic configuration checks.

        if (mOldLauncher.isReplicated() || mNewLauncher.isReplicated()) {
            // Both databases must not be concurrently modified by any other threads. This is
            // always possible with a replicated database.
            throw new IllegalStateException("Cannot rebuild a replicated database");
        }

        mOldLauncher.mMkdirs = false;
        mOldLauncher.dataFiles();

        File[] newFiles = mNewLauncher.dataFiles();

        if (newFiles != null) {
            for (File f : newFiles) {
                if (f.exists()) {
                    throw new IllegalStateException("New database already exists");
                }
            }
        }

        if (!mOldLauncher.mReadOnly) {
            // Open and close to perform recovery, and then re-open as read-only.
            mOldLauncher.open(false, null).close(); // destroy = false
            mOldLauncher.mReadOnly = true;
        }

        mOldDb = mOldLauncher.open(false, null);

        // The new database must have the same database id to ensure that the index id sequence
        // mask is the same, preventing collisions.
        mNewLauncher.mDatabaseId = mOldDb.databaseId();

        mNewDb = mNewLauncher.open(true, null); // destroy = true

        // First copy the registry key map to ensure that index ids (as supplied by
        // RK_NEXT_TREE_ID and RK_NEXT_TEMP_ID) follow the same sequence, and that new indexes
        // will effectively exist (initially empty).
        simpleCopy(mOldDb.registryKeyMap(), mNewDb.registryKeyMap());

        mOldDb.scanAllIndexes(oldIndex -> {
            long id = oldIndex.id();
            if (BTree.REGISTRY_ID <= id && id <= BTree.CURSOR_REGISTRY_ID) {
                // Don't copy any kind of registry, because the anyIndexById method rejects it.
                // There no reason to copy them again, or they don't need to be copied anyhow.
                return true;
            }

            Index newIndex = mNewDb.anyIndexById(id);

            if (!Arrays.equals(oldIndex.name(), newIndex.name())) {
                throw new AssertionError("New index name differs");
            }

            copy(oldIndex, newIndex);

            if (!BTree.isInternal(id)) {
                oldIndex.close();
                newIndex.close();
            }

            return true;
        });
    }

    private void copy(Index oldIndex, Index newIndex) throws IOException {
        if (!newIndex.isEmpty()) {
            throw new AssertionError("New index isn't empty");
        }

        if (mNumThreads <= 1) {
            simpleCopy(oldIndex, newIndex);
        } else {
            Index temp = mNewDb.parallelCopy(oldIndex, mNumThreads);
            mNewDb.rootSwap(temp, newIndex);
            temp.drop();
        }
    }

    private void simpleCopy(Index oldIndex, Index newIndex) throws IOException {
        int pageSize = mNewDb.stats().pageSize;
        byte[] buf = null;

        try (Cursor newCursor = newIndex.newCursor(Transaction.BOGUS)) {
            try (Cursor oldCursor = oldIndex.newCursor(Transaction.BOGUS)) {
                oldCursor.autoload(false);
                for (oldCursor.first(); oldCursor.key() != null; oldCursor.next()) {
                    newCursor.findNearby(oldCursor.key());

                    long length = oldCursor.valueLength();

                    if (length <= pageSize) {
                        oldCursor.load();
                        newCursor.store(oldCursor.value());
                        continue;
                    }

                    if (buf == null) {
                        buf = new byte[Math.max(mOldDb.stats().pageSize, pageSize)];
                    }

                    newCursor.valueLength(length);

                    long pos = 0;
                    while (true) {
                        int amt = oldCursor.valueRead(pos, buf, 0, buf.length);
                        newCursor.valueWrite(pos, buf, 0, amt);
                        pos += amt;
                        if (amt < buf.length) {
                            break;
                        }
                    }

                    if (pos != length) {
                        throw new AssertionError("Value isn't fully copied");
                    }
                }
            }
        }
    }
}
