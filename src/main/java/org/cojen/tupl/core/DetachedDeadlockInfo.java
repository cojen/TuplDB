/*
 *  Copyright (C) 2021 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.core;

import org.cojen.tupl.diag.DeadlockInfo;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final record DetachedDeadlockInfo(String desc, Object ownerAttachment) implements DeadlockInfo {
    private static final long serialVersionUID = 1L;

    @Override
    public Object row() {
        return null;
    }

    @Override
    public long indexId() {
        return 0;
    }

    @Override
    public byte[] indexName() {
        return null;
    }

    @Override
    public byte[] key() {
        return null;
    }

    @Override
    public String toString() {
        if (ownerAttachment == null) {
            return desc;
        }
        return desc + "; attachment: " + ownerAttachment;
    }
}
