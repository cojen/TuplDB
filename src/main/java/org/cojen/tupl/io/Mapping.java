/*
 *  Copyright 2013 Brian S O'Neill
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
