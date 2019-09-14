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

import org.cojen.tupl.DeadlockInfo;

import java.util.Arrays;
import java.util.Objects;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class CoreDeadlockInfo implements DeadlockInfo {
    private static final long serialVersionUID = 1L;

    long mIndexId;
    byte[] mIndexName;
    byte[] mKey;
    Object mAttachment;

    @Override
    public long getIndexId() {
        return mIndexId;
    }

    @Override
    public byte[] getIndexName() {
        return mIndexName;
    }

    @Override
    public byte[] getKey() {
        return mKey;
    }

    @Override
    public Object getOwnerAttachment() {
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
        if (this == obj) {
            return true;
        }
        if (obj instanceof CoreDeadlockInfo) {
            CoreDeadlockInfo other = (CoreDeadlockInfo) obj;
            return mIndexId == other.mIndexId
                && Arrays.equals(mIndexName, other.mIndexName)
                && Arrays.equals(mKey, other.mKey)
                && Objects.equals(mAttachment, other.mAttachment);
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder()
            .append('{')
            .append("indexId").append(": ").append(mIndexId)
            .append(", ");

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
