/*
 *  Copyright 2011 Brian S O'Neill
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
 * 
 *
 * @author Brian S O'Neill
 */
public interface OrderedView {
    /**
     * @return a new unpositioned cursor
     */
    public Cursor newCursor();

    public long count() throws IOException;

    /**
     * Returns true if an entry exists for the given key.
     *
     * @param key non-null key
     * @return true if non-null value exists for the given key
     */
    public boolean exists(byte[] key) throws IOException;

    /**
     * Returns true if a matching key-value entry exists.
     *
     * @param key non-null key
     * @param value value to compare to, which can be null
     * @return true if entry matches given key and value
     */
    public boolean exists(byte[] key, byte[] value) throws IOException;

    /**
     * Returns a copy of the value for the given key, or null if no matching
     * entry exists.
     *
     * @param key non-null key
     * @return copy of value, or null if entry doesn't exist
     */
    public byte[] get(byte[] key) throws IOException;

    /**
     * @param key non-null key
     * @param value value to store; pass null to delete
     */
    public void store(byte[] key, byte[] value) throws IOException;

    /**
     * @param key non-null key
     * @param value value to insert, which can be null
     * @return false if entry already exists
     */
    public boolean insert(byte[] key, byte[] value) throws IOException;

    /**
     * @param key non-null key
     * @param value value to insert; pass null to delete
     * @return false if no existing entry
     */
    public boolean replace(byte[] key, byte[] value) throws IOException;

    /**
     * @param key non-null key
     * @param oldValue expected existing value, which can be null
     * @param newValue new value to update to; pass null to delete
     * @return false if existing value doesn't match
     */
    public boolean update(byte[] key, byte[] oldValue, byte[] newValue) throws IOException;

    /**
     * @param key non-null key
     * @return false if no existing entry
     */
    public boolean delete(byte[] key) throws IOException;

    /**
     * @param key non-null key
     * @param value expected existing value, which can be null
     * @return false if existing value doesn't match
     */
    public boolean remove(byte[] key, byte[] value) throws IOException;

    public void clear() throws IOException;

    /**
     * Returns a sub-view, backed by this one, whose keys are greater than or
     * equal to the given key.
     */
    public OrderedView viewGe(byte[] key);

    /**
     * Returns a sub-view, backed by this one, whose keys are greater than the
     * given key.
     */
    public OrderedView viewGt(byte[] key);

    /**
     * Returns a sub-view, backed by this one, whose keys are less than or
     * equal to the given key.
     */
    public OrderedView viewLe(byte[] key);

    /**
     * Returns a sub-view, backed by this one, whose keys are less than the
     * given key.
     */
    public OrderedView viewLt(byte[] key);

    /**
     * Returns a sub-view, backed by this one, whose keys all start with the
     * given prefix.
     */
    public OrderedView viewPrefix(byte[] keyPrefix);

    /**
     * Returns a view, backed by this one, whose natural order is reversed.
     */
    public OrderedView viewReverse();
}
