/*
 *  Copyright 2012-2013 Brian S O'Neill
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
    private final byte[][] mKeys;

    DeadlockSet(Set<Lock> locks) {
        int size = locks.size();
        long[] indexIds = new long[size];
        byte[][] keys = new byte[size][];

        int i = 0;
        for (Lock lock : locks) {
            indexIds[i] = lock.mIndexId;
            byte[] key = lock.mKey;
            if (key != null) {
                key = key.clone();
            }
            keys[i] = key;
            i++;
        }

        mIndexIds = indexIds;
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
            b.append("key").append(": ").append(Utils.toHex(mKeys[i]));
            b.append('}');
        }
    }
}
