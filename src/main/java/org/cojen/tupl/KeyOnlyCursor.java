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
final class KeyOnlyCursor extends WrappedCursor<Cursor> {
    KeyOnlyCursor(Cursor source) {
        super(source);
        source.autoload(false);
    }

    @Override
    public long valueLength() throws IOException {
        return source.valueLength();
    }

    @Override
    public void valueLength(long length) throws IOException {
        if (length >= 0) {
            throw new ViewConstraintException();
        }
        source.store(null);
    }

    @Override
    public int valueRead(long pos, byte[] buf, int off, int len) throws IOException {
        return source.valueRead(pos, buf, off, 0);
    }

    @Override
    public InputStream newValueInputStream(long pos) throws IOException {
        return new InputStream() {
            @Override
            public int read() throws IOException {
                if (value() == null) {
                    throw new NoSuchValueException();
                }
                return -1;
            }
        };
    }

    @Override
    public InputStream newValueInputStream(long pos, int bufferSize) throws IOException {
        return newValueInputStream(pos);
    }

    @Override
    public OutputStream newValueOutputStream(long pos) throws IOException {
        throw new ViewConstraintException();
    }

    @Override
    public OutputStream newValueOutputStream(long pos, int bufferSize) throws IOException {
        return newValueOutputStream(pos);
    }

    @Override
    public byte[] value() {
        return KeyOnlyView.valueScrub(source.value());
    }

    @Override
    public boolean autoload(boolean mode) {
        return false;
    }

    @Override
    public boolean autoload() {
        return false;
    }

    @Override
    public LockResult load() throws IOException {
        return source.lock();
    }

    @Override
    public void store(byte[] value) throws IOException {
        KeyOnlyView.valueCheck(value);
        source.store(null);
    }

    @Override
    public void commit(byte[] value) throws IOException {
        KeyOnlyView.valueCheck(value);
        source.commit(null);
    }

    @Override
    public Cursor copy() {
        return new KeyOnlyCursor(source.copy());
    }
}
