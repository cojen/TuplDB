/*
 *  Copyright 2012 Brian S O'Neill
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

package org.cojen.tupl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Simple persistent bit map, used during PageArray snapshot. A memory mapped
 * file would be ideal on a 64-bit, but Java memory mapped I/O facilities are
 * worse than nothing.
 *
 * @author Brian S O'Neill
 */
class BitMapFile extends CauseCloseable {
    private final RandomAccessFile mRaf;

    private volatile Throwable mCause;

    public BitMapFile(File file) throws IOException {
        mRaf = new RandomAccessFile(file, "rw");
    }

    public boolean get(long index) throws IOException {
        try {
            long pos = index >> 3;
            int v;
            synchronized (this) {
                mRaf.seek(pos);
                v = mRaf.read();
            }
            return v < 0 ? false : (v & (1 << (index & 7))) != 0;
        } catch (IOException e) {
            throw Utils.rethrow(e, mCause);
        }
    }

    /**
     * @return old value
     */
    public boolean set(long index) throws IOException {
        try {
            long pos = index >> 3;
            int ov = 1 << (index & 7);
            synchronized (this) {
                mRaf.seek(pos);
                int v = mRaf.read();
                if (v < 0) {
                    v = 0;
                } else if ((v & ov) != 0) {
                    return true;
                }
                mRaf.seek(pos);
                mRaf.write(v | ov);
            }
            return false;
        } catch (IOException e) {
            throw Utils.rethrow(e, mCause);
        }
    }

    public void clear(long index) throws IOException {
        try {
            long pos = index >> 3;
            int av = ~(1 << (index & 7));
            synchronized (this) {
                mRaf.seek(pos);
                int v = mRaf.read();
                mRaf.seek(pos);
                // Always write a value, to support file pre-allocation.
                mRaf.write(v < 0 ? 0 : (v & av));
            }
        } catch (IOException e) {
            throw Utils.rethrow(e, mCause);
        }
    }

    @Override
    public void close() throws IOException {
        close(null);
    }

    @Override
    public void close(Throwable cause) throws IOException {
        mRaf.close();
        if (cause != null) {
            mCause = cause;
        }
    }
}
