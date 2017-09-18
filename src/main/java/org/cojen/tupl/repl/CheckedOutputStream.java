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

import java.io.IOException;
import java.io.OutputStream;

import java.util.zip.Checksum;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class CheckedOutputStream extends OutputStream {
    private final OutputStream mDest;
    private final Checksum mChecksum;

    CheckedOutputStream(OutputStream dest, Checksum checksum) {
        mDest = dest;
        mChecksum = checksum;
    }

    @Override
    public void write(int b) throws IOException {
        mDest.write(b);
        mChecksum.update(b);
    }

    @Override
    public void write(byte[] buf, int off, int len) throws IOException {
        mDest.write(buf, off, len);
        mChecksum.update(buf, off, len);
    }

    void writeChecksum() throws IOException {
        byte[] buf = new byte[4];
        Utils.encodeIntLE(buf, 0, (int) mChecksum.getValue());
        mDest.write(buf);
    }

    @Override
    public void close() throws IOException {
        mDest.close();
    }
}
