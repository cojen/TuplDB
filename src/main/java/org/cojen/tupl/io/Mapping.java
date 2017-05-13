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

package org.cojen.tupl.io;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import java.nio.ByteBuffer;

/**
 * 
 *
 * @author Brian S O'Neill
 */
abstract class Mapping implements Closeable {
    static Mapping open(File file, boolean readOnly, long position, int size) throws IOException {
        return new NioMapping(file, readOnly, position, size);
    }

    abstract void read(int start, byte[] b, int off, int len);

    abstract void read(int start, ByteBuffer b);

    abstract void write(int start, byte[] b, int off, int len);

    abstract void write(int start, ByteBuffer b);

    abstract void sync(boolean metadata) throws IOException;
}
