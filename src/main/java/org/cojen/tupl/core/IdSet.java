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

package org.cojen.tupl.core;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.cojen.tupl.Transaction;

/**
 * Used to detect duplicate page ids in the free list.
 *
 * @author Brian S. O'Neill
 * @see LocalDatabase.FreeListScan
 */
final class IdSet implements Closeable {
    private static final int MAX_SET_SIZE = 1_000_000;

    private final TempFileManager mTempFileManager;

    private LHashTable.Set mSet;

    private BTree mTempTree;
    private File mTempFile;

    IdSet(TempFileManager tfm) {
        mTempFileManager = tfm;
        mSet = new LHashTable.Set(1024);
    }

    /**
     * @return false if already added
     */
    public boolean add(long id) throws IOException {
        LHashTable.Set set = mSet;

        if (set != null) {
            if (set.size() < MAX_SET_SIZE || mTempFileManager == null) {
                return set.insert(Utils.scramble(id)) != null;
            }

            // Reduce memory usage by moving to a temporary tree.

            var launcher = new Launcher();
            mTempTree = LocalDatabase.openTemp(mTempFileManager, launcher);
            mTempFile = launcher.mBaseFile;

            var key = new byte[8];

            set.traverse(entry -> {
                Utils.encodeLongBE(key, 0, Utils.unscramble(entry.key));
                mTempTree.store(Transaction.BOGUS, key, Utils.EMPTY_BYTES);
                return true;
            });

            mSet = null;
        }

        var key = new byte[8];
        Utils.encodeLongBE(key, 0, id);

        return mTempTree.insert(Transaction.BOGUS, key, Utils.EMPTY_BYTES);
    }

    @Override
    public void close() throws IOException {
        if (mTempTree != null) {
            mTempTree.mDatabase.close();
            mTempFileManager.deleteTempFile(mTempFile);
        }
    }
}
