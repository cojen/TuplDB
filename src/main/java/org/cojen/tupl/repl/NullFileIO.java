/*
 *  Copyright 2019 Cojen.org
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

import java.nio.ByteBuffer;

import org.cojen.tupl.io.FileIO;
import org.cojen.tupl.io.LengthOption;

/**
 * Writes go nowhere and reads are unsupported.
 *
 * @author Brian S O'Neill
 */
final class NullFileIO extends FileIO {
    static final NullFileIO THE = new NullFileIO();

    private NullFileIO() {
    }

    @Override
    public boolean isDirectIO() {
        return false;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public long length() {
        return 0;
    }

    @Override
    public void truncateLength(long length) {
    }

    @Override
    public void expandLength(long length, LengthOption option) {
    }

    @Override
    public void read(long pos, byte[] buf, int offset, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void read(long pos, ByteBuffer bb) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(long pos, byte[] buf, int offset, int length) {
    }

    @Override
    public void write(long pos, ByteBuffer bb) {
    }

    @Override
    public void map() {
    }

    @Override
    public void remap() {
    }

    @Override
    public void unmap() {
    }

    @Override
    public void sync(boolean metadata) {
    }

    @Override
    public void close(Throwable cause) {
    }
}
