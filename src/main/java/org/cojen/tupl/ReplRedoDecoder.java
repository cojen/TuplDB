/*
 *  Copyright (C) 2011-2017 Cojen.org
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

package org.cojen.tupl;

import java.io.IOException;

import org.cojen.tupl.ext.ReplicationManager;

import org.cojen.tupl.util.Latch;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see ReplRedoEngine
 */
final class ReplRedoDecoder extends RedoDecoder {
    volatile boolean mDeactivated;

    ReplRedoDecoder(ReplicationManager manager,
                    long initialPosition, long initialTxnId,
                    Latch decodeLatch)
    {
        super(false, initialTxnId, new In(initialPosition, manager), decodeLatch);
    }

    @Override
    boolean verifyTerminator(DataIn in) {
        // No terminators to verify.
        return true;
    }

    static final class In extends DataIn {
        private final ReplicationManager mManager;

        In(long position, ReplicationManager manager) {
            this(position, manager, 64 << 10);
        }

        In(long position, ReplicationManager manager, int bufferSize) {
            super(position, bufferSize);
            mManager = manager;
        }

        @Override
        int doRead(byte[] buf, int off, int len) throws IOException {
            return mManager.read(buf, off, len);
        }

        @Override
        public void close() throws IOException {
            // Nothing to close.
        }
    }
}
