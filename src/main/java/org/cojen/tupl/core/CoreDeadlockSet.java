/*
 *  Copyright 2019 Cojen.org
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.core;

import java.io.Serializable;

import org.cojen.tupl.DeadlockSet;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class CoreDeadlockSet implements DeadlockSet, Serializable {
    private static final long serialVersionUID = 1L;

    private final OwnerInfo[] mInfoSet;

    CoreDeadlockSet(OwnerInfo[] infoSet) {
        mInfoSet = infoSet;
    }

    /**
     * @return number of elements in the set
     */
    @Override
    public int size() {
        return mInfoSet.length;
    }

    /**
     * @return the lock request index id at the given set position
     * @throws IndexOutOfBoundsException
     */
    @Override
    public long getIndexId(int pos) {
        return mInfoSet[pos].mIndexId;
    }

    /**
     * @return the lock request index name at the given set position, possibly null
     * @throws IndexOutOfBoundsException
     */
    @Override
    public byte[] getIndexName(int pos) {
        return mInfoSet[pos].mIndexName;
    }

    /**
     * @return the lock request index name string at the given set position, possibly null
     * @throws IndexOutOfBoundsException
     */
    @Override
    public String getIndexNameString(int pos) {
        return Utils.utf8(getIndexName(pos));
    }

    /**
     * @return the lock request key at the given set position
     * @throws IndexOutOfBoundsException
     */
    @Override
    public byte[] getKey(int pos) {
        return mInfoSet[pos].mKey;
    }

    /**
     * @return the lock owner attachment at the given set position, possibly null
     * @throws IndexOutOfBoundsException
     */
    @Override
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

    private void appendMembers(StringBuilder b) {
        for (int i=0; i<mInfoSet.length; i++) {
            OwnerInfo info = mInfoSet[i];
            if (i > 0) {
                b.append(", ");
            }
            b.append('{');
            b.append("indexId").append(": ").append(info.mIndexId);
            b.append(", ");

            String name = Utils.utf8(info.mIndexName);
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
