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

import org.cojen.tupl.Index;

import org.cojen.tupl.diag.VerificationObserver;

import org.cojen.tupl.util.Latch;

/**
 * Wraps a VerificationObserver and tracks additional state as needed by verification.
 *
 * @author Brian S. O'Neill
 */
final class VerifyObserver extends VerificationObserver {
    private final VerificationObserver mWrapped;

    private volatile boolean mFailed;

    private boolean mLatched;

    /**
     * @param wrapped if null, a default is used
     */
    VerifyObserver(VerificationObserver wrapped) {
        if (wrapped == null) {
            wrapped = new VerificationObserver();
        }
        mWrapped = wrapped;
    }

    @Override
    public boolean indexBegin(Index index, int height) {
        return mWrapped.indexBegin(index, height);
    }

    @Override
    public boolean indexComplete(Index index, boolean passed, String message) {
        return mWrapped.indexComplete(index, passed & !mFailed, message);
    }

    @Override
    public boolean indexNodePassed(long id, int level,
                                   int entryCount, int freeBytes, int largeValueCount)
    {
        return mWrapped.indexNodePassed(id, level, entryCount, freeBytes, largeValueCount);
    }

    @Override
    public boolean indexNodeFailed(long id, int level, String message) {
        mFailed = true;
        return mWrapped.indexNodeFailed(id, level, message);
    }

    boolean passed() {
        return !mFailed;
    }

    /**
     * @see Node#verifyTreeNode
     */
    void heldShared() {
        mLatched = true;
    }

    /**
     * @see Node#verifyTreeNode
     */
    void releaseShared(Latch latch) {
        if (mLatched) {
            mLatched = false;
            latch.releaseShared();
        }
    }
}
