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

import java.io.EOFException;
import java.io.InputStream;
import java.io.IOException;

import java.util.zip.Checksum;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class CheckedInputStream extends InputStream {
    private final InputStream mSource;
    private final Checksum mChecksum;
    private long mRemaining;

    CheckedInputStream(InputStream source, Checksum checksum, long length) {
        mSource = source;
        mChecksum = checksum;
        mRemaining = length;
    }

    @Override
    public int read() throws IOException {
        byte[] buf = new byte[1];
        return read(buf) <= 0 ? -1 : (buf[0] & 0xff);
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        int amt = mSource.read(buf, off, len <= mRemaining ? len : (int) mRemaining);

        if (amt <= 0) {
            if (len == 0) {
                return 0;
            }
            if (mRemaining <= 0) {
                return -1;
            }
            throw new EOFException("Remaining length: " + mRemaining);
        }

        mChecksum.update(buf, off, amt);

        if ((mRemaining -= amt) <= 0) {
            byte[] expectBuf = new byte[4];
            try {
                Utils.readFully(mSource, expectBuf, 0, expectBuf.length);
            } catch (EOFException e) {
                throw new EOFException("Checksum is missing");
            }
            int expect = Utils.decodeIntLE(expectBuf, 0);
            int actual = (int) mChecksum.getValue();
            if (expect != actual) {
                throw new IOException("Checksum mismatch: " + expect + " != " + actual);
            }
        }

        return amt;
    }

    @Override
    public void close() throws IOException {
        mSource.close();
    }
}
