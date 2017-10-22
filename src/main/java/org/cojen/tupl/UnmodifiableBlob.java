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

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class UnmodifiableBlob implements Blob {
    private final Blob mSource;

    UnmodifiableBlob(Blob source) {
        mSource = source;
    }

    @Override
    public long length() throws IOException {
        return mSource.length();
    }

    @Override
    public void setLength(long length) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public int read(long pos, byte[] buf, int off, int len) throws IOException {
        return mSource.read(pos, buf, off, len);
    }

    @Override
    public void write(long pos, byte[] buf, int off, int len) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public InputStream newInputStream(long pos) throws IOException {
        return mSource.newInputStream(pos);
    }

    @Override
    public InputStream newInputStream(long pos, int bufferSize) throws IOException {
        return mSource.newInputStream(pos, bufferSize);
    }

    @Override
    public OutputStream newOutputStream(long pos) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public OutputStream newOutputStream(long pos, int bufferSize) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public void close() {
        mSource.close();
    }
}
