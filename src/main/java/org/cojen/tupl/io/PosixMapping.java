/*
 *  Copyright 2015 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl.io;

import java.io.IOException;

import java.nio.ByteBuffer;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class PosixMapping extends Mapping {
    private final DirectAccess mDirectAccess;
    private final long mAddr;
    private final int mSize;

    PosixMapping(int fd, boolean readOnly, long position, int size) throws IOException {
        mDirectAccess = new DirectAccess();
        int prot = readOnly ? 1 : (1 | 2); // PROT_READ | PROT_WRITE
        int flags = 1; // MAP_SHARED
        mAddr = PosixFileIO.mmapFd(size, prot, flags, fd, position);
        mSize = size;
    }

    @Override
    void read(int start, byte[] b, int off, int len) {
        mDirectAccess.prepare(mAddr + start, len).get(b, off, len);
    }

    @Override
    void read(int start, ByteBuffer dst) {
        dst.put(mDirectAccess.prepare(mAddr + start, dst.remaining()));
    }

    @Override
    void write(int start, byte[] b, int off, int len) {
        mDirectAccess.prepare(mAddr + start, len).put(b, off, len);
    }

    @Override
    void write(int start, ByteBuffer src) {
        mDirectAccess.prepare(mAddr + start, src.remaining()).put(src);
    }

    @Override
    void sync(boolean metadata) throws IOException {
        PosixFileIO.msyncAddr(mAddr, mSize);
    }

    @Override
    public void close() throws IOException {
        PosixFileIO.munmapAddr(mAddr, mSize);
    }
}
