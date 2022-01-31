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

package org.cojen.tupl.diag;

import java.io.Serializable;

import org.cojen.tupl.DeadlockException;

import org.cojen.tupl.core.Utils;

/**
 * Lock request which was in a deadlock.
 *
 * @author Brian S O'Neill
 * @see DeadlockException
 */
public interface DeadlockInfo extends Serializable {
    /**
     * @return the lock request row object, possibly null
     */
    public Object row();

    /**
     * @return the lock request index id
     */
    public long indexId();

    /**
     * @return the lock request index name, possibly null
     */
    public byte[] indexName();

    /**
     * @return the lock request index name string, possibly null
     */
    public default String indexNameString() {
        return Utils.utf8(indexName());
    }

    /**
     * @return the lock request key
     */
    public byte[] key();

    /**
     * @return the lock owner attachment, possibly null
     */
    public Object ownerAttachment();
}
