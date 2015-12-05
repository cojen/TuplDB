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

import java.io.IOException;
import java.io.OutputStream;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class UnmodifiableStream extends WrappedStream {
    UnmodifiableStream(Stream source) {
        super(source);
    }

    @Override
    public LockResult open(Transaction txn, byte[] key) throws IOException {
        return mSource.open(txn, key);
    }

    @Override
    public void setLength(long length) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public void write(long pos, byte[] buf, int off, int len) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public OutputStream newOutputStream(long pos) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public OutputStream newOutputStream(long pos, int bufferSize) throws IOException {
        throw new UnmodifiableViewException();
    }
}
