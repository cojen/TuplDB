/*
 *  Copyright 2013-2015 Cojen.org
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

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * 
 *
 * @author Brian S O'Neill
 */
abstract class WrappedStream implements Stream {
    final Stream mSource;

    WrappedStream(Stream source) {
        mSource = source;
    }

    @Override
    public Transaction link(Transaction txn) {
        return mSource.link(txn);
    }

    @Override
    public Transaction link() {
        return mSource.link();
    }

    @Override
    public long length() throws IOException {
        return mSource.length();
    }

    @Override
    public void setLength(long length) throws IOException {
        mSource.setLength(length);
    }

    @Override
    public int read(long pos, byte[] buf, int off, int len) throws IOException {
        return mSource.read(pos, buf, off, len);
    }

    @Override
    public void write(long pos, byte[] buf, int off, int len) throws IOException {
        mSource.write(pos, buf, off, len);
    }

    public InputStream newInputStream(long pos) throws IOException {
        return mSource.newInputStream(pos);
    }

    public InputStream newInputStream(long pos, int bufferSize) throws IOException {
        return mSource.newInputStream(pos, bufferSize);
    }

    public OutputStream newOutputStream(long pos) throws IOException {
        return mSource.newOutputStream(pos);
    }

    public OutputStream newOutputStream(long pos, int bufferSize) throws IOException {
        return mSource.newOutputStream(pos, bufferSize);
    }

    @Override
    public void close() throws IOException {
        mSource.close();
    }
}
