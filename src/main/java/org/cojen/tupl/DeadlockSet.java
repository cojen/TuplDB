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

import java.nio.charset.StandardCharsets;

import java.util.Set;

/**
 * Set of lock requests which were in a deadlock.
 *
 * @author Brian S O'Neill
 * @see DeadlockException
 */
public final class DeadlockSet implements Serializable {
    private static final long serialVersionUID = 1L;

    private final OwnerInfo[] mInfoSet;

    DeadlockSet(OwnerInfo[] infoSet) {
        mInfoSet = infoSet;
    }

    /**
     * @return number of elements in the set
     */
    public int size() {
        return mInfoSet.length;
    }

    /**
     * @return the lock request index id at the given set position
     * @throws IndexOutOfBoundsException
     */
    public long getIndexId(int pos) {
        return mInfoSet[pos].mIndexId;
    }

    /**
     * @return the lock request index name at the given set position, possibly null
     * @throws IndexOutOfBoundsException
     */
    public byte[] getIndexName(int pos) {
        return mInfoSet[pos].mIndexName;
    }

    /**
     * @return the lock request index name string at the given set position, possibly null
     * @throws IndexOutOfBoundsException
     */
    public String getIndexNameString(int pos) {
        return indexNameString(getIndexName(pos));
    }

    private static String indexNameString(byte[] name) {
        if (name == null) {
            return null;
        }
        return new String(name, StandardCharsets.UTF_8);
    }

    /**
     * @return the lock request key at the given set position
     * @throws IndexOutOfBoundsException
     */
    public byte[] getKey(int pos) {
        return mInfoSet[pos].mKey;
    }

    /**
     * @return the lock owner attachment at the given set position, possibly null
     * @throws IndexOutOfBoundsException
     */
    public Object getOwnerAttachment(int pos) {
        return mInfoSet[pos].mAttachment;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append('[');
        appendMembers(b);
        return b.append(']').toString();
    }

    void appendMembers(StringBuilder b) {
        for (int i=0; i<mInfoSet.length; i++) {
            OwnerInfo info = mInfoSet[i];
            if (i > 0) {
                b.append(", ");
            }
            b.append('{');
            b.append("indexId").append(": ").append(info.mIndexId);
            b.append(", ");

            String name = indexNameString(info.mIndexName);
            if (name != null) {
                b.append("indexName").append(": ").append(name);
                b.append(", ");
            }

            b.append("key").append(": ").append(Utils.toHex(info.mKey));

            Object att = info.mAttachment;
            if (att != null) {
                b.append(", ");
                b.append("attachment").append(": ").append(att);
            }

            b.append('}');
        }
    }

    static class OwnerInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        long mIndexId;
        byte[] mIndexName;
        byte[] mKey;
        Object mAttachment;
    }
}
