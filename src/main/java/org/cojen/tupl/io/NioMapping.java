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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import java.nio.channels.FileChannel;

import static java.nio.channels.FileChannel.MapMode.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class NioMapping extends DirectMapping {
    private final Arena mArena;
    private final RandomAccessFile mRaf;
    private final FileChannel mChannel;
    private final MemorySegment mSegment;

    NioMapping(File file, boolean readOnly, long position, int size) throws IOException {
        this(file, readOnly, position, size, Arena.ofShared(), null, null, null);
    }

    private NioMapping(File file, boolean readOnly, long position, int size, Arena arena,
                       RandomAccessFile raf, FileChannel channel, MemorySegment segment)
        throws IOException
    {
        super((segment =
               (channel =
                (raf = new RandomAccessFile(file, readOnly ? "r" : "rw")).getChannel())
               .map(readOnly ? READ_ONLY : READ_WRITE, position, size, arena)).address(), size);
        mArena = arena;
        mRaf = raf;
        mChannel = channel;
        mSegment = segment;
    }

    @Override
    void sync(boolean metadata) throws IOException {
        try {
            mSegment.force();
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
        mChannel.force(metadata);
    }

    @Override
    public void close() throws IOException {
        try {
            mArena.close();
        } catch (IllegalStateException e) {
            // Already closed.
        }
        mRaf.close();
    }
}
