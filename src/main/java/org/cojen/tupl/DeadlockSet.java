/*
 *  Copyright (C) 2011-2017 Cojen.org
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

package org.cojen.tupl;

/**
 * Set of lock requests which were in a deadlock.
 *
 * @author Brian S O'Neill
 * @see DeadlockException
 */
public interface DeadlockSet {
    /**
     * @return number of elements in the set
     */
    public int size();

    /**
     * @return the lock request index id at the given set position
     * @throws IndexOutOfBoundsException
     */
    public long getIndexId(int pos);

    /**
     * @return the lock request index name at the given set position, possibly null
     * @throws IndexOutOfBoundsException
     */
    public byte[] getIndexName(int pos);

    /**
     * @return the lock request index name string at the given set position, possibly null
     * @throws IndexOutOfBoundsException
     */
    public String getIndexNameString(int pos);

    /**
     * @return the lock request key at the given set position
     * @throws IndexOutOfBoundsException
     */
    public byte[] getKey(int pos);

    /**
     * @return the lock owner attachment at the given set position, possibly null
     * @throws IndexOutOfBoundsException
     */
    public Object getOwnerAttachment(int pos);
}
