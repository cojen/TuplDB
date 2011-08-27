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

import java.util.Arrays;

/**
 * Container for retrieving key and value pair from {@link Cursor#getEntry}
 * methods.
 *
 * @author Brian S O'Neill
 */
public class Entry {
    public byte[] key;
    public byte[] value;

    public final void clear() {
        key = null;
        value = null;
    }

    /**
     * Compares Entry keys, treating null as high.
     */
    public final int compareKeys(Entry other) {
        byte[] thisKey = key;
        byte[] otherKey = other.key;
        return (thisKey == null)
            ? (otherKey == null ? 0 : 1)
            : (otherKey == null
               ? -1
               : Utils.compareKeys(thisKey, 0, thisKey.length, otherKey, 0, otherKey.length));
    }

    /**
     * Returns a deep copy of this Entry.
     */
    public final Entry copy() {
        Entry copy = new Entry();
        if (key != null) {
            copy.key = key.clone();
        }
        if (value != null) {
            copy.value = value.clone();
        }
        return copy;
    }

    /**
     * Computes a hash code from the key and value.
     */
    @Override
    public final int hashCode() {
        return Arrays.hashCode(key) ^ Arrays.hashCode(value);
    }

    /**
     * Compares Entry keys and values for equality.
     */
    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof Entry) {
            Entry other = (Entry) obj;
            return Arrays.equals(key, other.key) && Arrays.equals(value, other.value);
        }
        return false;
    }

    // Called by TreeCursor.
    byte[] get(TreeCursorFrame leaf, TreeNode node) {
        int pos = leaf.mNodePos;
        if (pos < 0) {
            key = leaf.mNotFoundKey.clone();
            value = null;
            return null;
        } else {
            node.retrieveLeafEntry(pos, this);
            return Utils.EMPTY_BYTES;
        }
    }

    static final Entry GET_KEY = new Entry() {
        @Override
        byte[] get(TreeCursorFrame leaf, TreeNode node) {
            int pos = leaf.mNodePos;
            return pos < 0 ? (leaf.mNotFoundKey.clone()) : node.retrieveLeafKey(pos);
        }
    };

    static final Entry GET_VALUE = new Entry() {
        @Override
        byte[] get(TreeCursorFrame leaf, TreeNode node) {
            int pos = leaf.mNodePos;
            return pos < 0 ? null : node.retrieveLeafValue(pos);
        }
    };
}
