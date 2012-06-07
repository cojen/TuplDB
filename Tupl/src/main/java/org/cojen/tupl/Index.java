/*
 *  Copyright 2011-2012 Brian S O'Neill
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

/**
 * Mapping of keys to values, ordered by key, in lexicographical
 * order. Although Java bytes are signed, they are treated as unsigned for
 * ordering purposes. The natural order of an index cannot be changed.
 *
 * @author Brian S O'Neill
 * @see Database
 */
public interface Index extends View {
    /**
     * @return randomly assigned, unique non-zero identifier for this index
     */
    public long getId();

    /**
     * @return unique user-specified index name
     */
    public byte[] getName();

    /**
     * @return name decoded as UTF-8
     */
    public String getNameString();

    /**
     * {@inheritDoc}
     */
    public Cursor newCursor(Transaction txn);

    /**
     * {@inheritDoc}
     */
    public byte[] load(Transaction txn, byte[] key) throws IOException;

    /**
     * {@inheritDoc}
     */
    public void store(Transaction txn, byte[] key, byte[] value) throws IOException;

    /**
     * {@inheritDoc}
     */
    public boolean insert(Transaction txn, byte[] key, byte[] value) throws IOException;

    /**
     * {@inheritDoc}
     */
    public boolean replace(Transaction txn, byte[] key, byte[] value) throws IOException;

    /**
     * {@inheritDoc}
     */
    public boolean update(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException;

    /**
     * {@inheritDoc}
     */
    public boolean delete(Transaction txn, byte[] key) throws IOException;

    /**
     * {@inheritDoc}
     */
    public boolean remove(Transaction txn, byte[] key, byte[] value) throws IOException;
}
