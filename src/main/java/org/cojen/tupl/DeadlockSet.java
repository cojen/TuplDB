/*
 *  Copyright 2012-2015 Cojen.org
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
import java.io.Serializable;

import java.util.Set;

/**
 * Set of lock requests which were in a deadlock.
 *
 * @author Brian S O'Neill
 * @see DeadlockException
 */
public final class DeadlockSet implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long[] mIndexIds;
    private final byte[][] mIndexNames;
    private final byte[][] mKeys;

    DeadlockSet(long[] indexIds, byte[][] indexNames, byte[][] keys) {
        mIndexIds = indexIds;
        mIndexNames = indexNames;
        mKeys = keys;
    }

    /**
     * @return number of elements in the set
     */
    public int size() {
        return mIndexIds.length;
    }

    /**
     * @return the lock request index id at the given set position
     * @throws IndexOutOfBoundsException
     */
    public long getIndexId(int pos) {
        return mIndexIds[pos];
    }

    /**
     * @return the lock request index name at the given set position, possibly null
     * @throws IndexOutOfBoundsException
     */
    public byte[] getIndexName(int pos) {
        return mIndexNames[pos];
    }

    /**
     * @return the lock request index name string at the given set position, possibly null
     * @throws IndexOutOfBoundsException
     */
    public String getIndexNameString(int pos) {
        byte[] name = mIndexNames[pos];
        if (name == null) {
            return null;
        }
        try {
            return new String(name, "UTF-8");
        } catch (IOException e) {
            return new String(name);
        }
    }

    /**
     * @return the lock request key at the given set position
     * @throws IndexOutOfBoundsException
     */
    public byte[] getKey(int pos) {
        return mKeys[pos];
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append('[');
        appendMembers(b);
        return b.append(']').toString();
    }

    void appendMembers(StringBuilder b) {
        for (int i=0; i<mIndexIds.length; i++) {
            if (i > 0) {
                b.append(", ");
            }
            b.append('{');
            b.append("indexId").append(": ").append(mIndexIds[i]);
            b.append(", ");

            String name = getIndexNameString(i);
            if (name != null) {
                b.append("indexName").append(": ").append(name);
                b.append(", ");
            }

            b.append("key").append(": ").append(Utils.toHex(mKeys[i]));
            b.append('}');
        }
    }
}
