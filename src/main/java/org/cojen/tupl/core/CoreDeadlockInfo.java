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

import org.cojen.tupl.diag.DeadlockInfo;

import java.util.Arrays;
import java.util.Objects;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public final class CoreDeadlockInfo implements DeadlockInfo {
    private static final long serialVersionUID = 1L;

    Object mRow;
    long mIndexId;
    byte[] mIndexName;
    byte[] mKey;
    Object mAttachment;

    CoreDeadlockInfo() {
    }

    public CoreDeadlockInfo(Object row, long indexId, byte[] indexName, byte[] key, Object att) {
        mRow = row;
        mIndexId = indexId;
        mIndexName = indexName;
        mKey = key;
        mAttachment = att;
    }

    @Override
    public Object row() {
        return mRow;
    }

    @Override
    public long indexId() {
        return mIndexId;
    }

    @Override
    public byte[] indexName() {
        return mIndexName;
    }

    @Override
    public byte[] key() {
        return mKey;
    }

    @Override
    public Object ownerAttachment() {
        return mAttachment;
    }

    @Override
    public int hashCode() {
        int hash = (int) mIndexId;
        hash = hash * 31 + Arrays.hashCode(mKey);
        hash = hash * 31 + Objects.hashCode(mAttachment);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || obj instanceof CoreDeadlockInfo other
            && mIndexId == other.mIndexId
            && Arrays.equals(mIndexName, other.mIndexName)
            && Arrays.equals(mKey, other.mKey)
            && Objects.equals(mAttachment, other.mAttachment);
    }

    @Override
    public String toString() {
        var b = new StringBuilder().append('{');

        if (mRow != null) {
            b.append("row").append(": ").append(mRow).append(", ");
        }

        b.append("indexId").append(": ").append(mIndexId).append(", ");

        String name = Utils.utf8(mIndexName);
        if (name != null) {
            b.append("indexName").append(": ").append(name);
            b.append(", ");
        }

        b.append("key").append(": ").append(Utils.toHex(mKey));

        Object att = mAttachment;
        if (att != null) {
            b.append(", ");
            b.append("attachment").append(": ").append(att);
        }

        return b.append('}').toString();
    }
}
