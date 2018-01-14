/*
 *  Copyright 2016 Cojen.org
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

import java.io.InterruptedIOException;
import java.io.IOException;

/**
 * Utility for sorting and filling up new indexes.
 *
 * @author Brian S O'Neill
 * @see Database
 */
public interface Sorter {
    /**
     * Add an entry into the sorter. If multiple entries are added with matching keys, only the
     * last one added is kept.
     *
     * @throws IllegalStateException if sort is finishing in another thread
     * @throws InterruptedIOException if reset by another thread
     */
    public void add(byte[] key, byte[] value) throws IOException;

    /**
     * Finish sorting the entries, and return a temporary index with the results.
     *
     * @throws IllegalStateException if sort is finishing in another thread
     * @throws InterruptedIOException if reset by another thread
     */
    public Index finish() throws IOException;

    /**
     * Discards all the entries and frees up space in the database. Can be called to interrupt
     * any sort which is in progress.
     */
    public void reset() throws IOException;
}
