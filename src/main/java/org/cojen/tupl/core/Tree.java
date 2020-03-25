/*
 *  Copyright (C) 2019 Cojen.org
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.cojen.tupl.CompactionObserver;
import org.cojen.tupl.Database;
import org.cojen.tupl.Index;
import org.cojen.tupl.VerificationObserver;

import org.cojen.tupl.views.ViewUtils;

/**
 * Base class for tree implementations. Only BTree for now.
 *
 * @author Brian S O'Neill
 */
abstract class Tree implements Index {
    // Reserved internal index ids.
    static final int
        REGISTRY_ID = 0,
        REGISTRY_KEY_MAP_ID = 1,
        CURSOR_REGISTRY_ID = 2,
        FRAGMENTED_TRASH_ID = 3,
        PREPARED_TXNS_ID = 4;

    static boolean isInternal(long id) {
        return (id & ~0xff) == 0;
    }

    /**
     * Returns a view which can be passed to an observer. Internal trees are returned as
     * unmodifiable.
     */
    abstract Index observableView();

    /**
     * @param view view to pass to observer
     * @return false if compaction should stop
     */
    abstract boolean compactTree(Index view, long highestNodeId, CompactionObserver observer)
        throws IOException;

    @Override
    public final boolean verify(VerificationObserver observer) throws IOException {
        if (observer == null) {
            observer = new VerificationObserver();
        }
        Index view = observableView();
        observer.failed = false;
        verifyTree(view, observer);
        boolean passed = !observer.failed;
        observer.indexComplete(view, passed, null);
        return passed;
    }

    /**
     * @param view view to pass to observer
     * @return false if should stop
     */
    abstract boolean verifyTree(Index view, VerificationObserver observer) throws IOException;

    /**
     * Count the number of cursors bound to the tree.
     *
     * @param strict pass false to fail-fast when trying to latch nodes, preventing deadlocks
     */
    abstract long countCursors(boolean strict);

    abstract void writeCachePrimer(DataOutput dout) throws IOException;

    abstract void applyCachePrimer(DataInput din) throws IOException;

    static final void skipCachePrimer(DataInput din) throws IOException {
        while (true) {
            int len = din.readUnsignedShort();
            if (len == 0xffff) {
                break;
            }
            while (len > 0) {
                int amt = din.skipBytes(len);
                if (amt <= 0) {
                    break;
                }
                len -= amt;
            }
        }
    }

    abstract boolean isMemberOf(Database db);

    abstract boolean isUserOf(Tree tree);
 
    /**
     * @param newName not cloned
     * @param redoTxnId non-zero if rename is performed by recovery
     */
    abstract void rename(byte[] newName, long redoTxnId) throws IOException;

    @Override
    public final void drop() throws IOException {
        drop(true).run();
    }

    /**
     * @return delete task
     */
    abstract Runnable drop(boolean mustBeEmpty) throws IOException;

    /**
     * Close any kind of index, even an internal one.
     */
    abstract void forceClose() throws IOException;

    @Override
    public String toString() {
        return ViewUtils.toString(this);
    }
}
