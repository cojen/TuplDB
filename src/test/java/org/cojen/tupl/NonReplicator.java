/*
 *  Copyright 2020 Cojen.org
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

import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Implementation fails to write anything.
 *
 * @author Brian S O'Neill
 */
public class NonReplicator extends SocketReplicator {
    public NonReplicator() throws IOException {
        asReplica();
    }

    public synchronized void asReplica() throws IOException {
        if (mOutput != null) {
            mOutput.close();
            mOutput = null;
        }

        mInput = new InputStream() {
            private boolean mClosed;

            @Override
            public synchronized int read() throws IOException {
                while (!mClosed) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        throw new InterruptedIOException();
                    }
                }
                return -1;
            }

            @Override
            public synchronized void close() {
                mClosed = true;
                notify();
            }
        };

        notifyAll();
    }

    public synchronized void asLeader() throws IOException {
        if (mInput != null) {
            mInput.close();
            mInput = null;
        }

        mOutput = OutputStream.nullOutputStream();

        notifyAll();
    }

    @Override
    protected long positionCheck(long position) {
        // Allow any position that's requested.
        return position;
    }
}
