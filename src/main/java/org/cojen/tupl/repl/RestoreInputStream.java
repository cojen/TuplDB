/*
 *  Copyright (C) 2017 Cojen.org
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

package org.cojen.tupl.repl;

import java.io.InputStream;
import java.io.IOException;

/**
 * @author Brian S O'Neill
 * @see DatabaseStreamReplicator
 */
final class RestoreInputStream extends InputStream {
    private final InputStream mSource;

    private volatile long mReceived;
    private volatile boolean mFinished;

    RestoreInputStream(InputStream source) {
        mSource = source;
    }

    /**
     * Returns the amount of bytes received.
     */
    public long received() {
        return mReceived;
    }

    public boolean isFinished() {
        return mFinished;
    }

    @Override
    public int read() throws IOException {
        int b = mSource.read();
        if (b < 0) {
            mFinished = true;
        } else {
            mReceived++;
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int amt = mSource.read(b, off, len);
        if (amt < 0) {
            mFinished = true;
        } else {
            mReceived += amt;
        }
        return amt;
    }

    @Override
    public long skip(long n) throws IOException {
        n = mSource.skip(n);
        if (n < 0) {
            mFinished = true;
        } else {
            mReceived += n;
        }
        return n;
    }

    @Override
    public int available() throws IOException {
        return mSource.available();
    }

    @Override
    public void close() throws IOException {
        mFinished = true;
        mSource.close();
    }
}
