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

import java.util.Collections;
import java.util.Set;

import org.cojen.tupl.diag.DeadlockInfo;

/**
 * Thrown when a lock request by a {@linkplain Transaction transaction} timed out due to a
 * deadlock, or a trivial deadlock was quickly detected. Deadlocks can be prevented by locking
 * records in a consistent order. Cases of "self deadlock" when using multiple transactions in
 * one thread are not detected, and a regular timeout exception is thrown instead.
 *
 * @author Brian S O'Neill
 */
public class DeadlockException extends LockTimeoutException {
    private static final long serialVersionUID = 1L;

    private final boolean mGuilty;
    private final Set<DeadlockInfo> mSet;

    /**
     * @param nanosTimeout negative is interpreted as infinite wait
     */
    public DeadlockException(long nanosTimeout) {
        this(nanosTimeout, null, false, null);
    }

    /**
     * @param nanosTimeout negative is interpreted as infinite wait
     */
    public DeadlockException(long nanosTimeout, Object attachment, boolean guilty) {
        this(nanosTimeout, attachment, guilty, null);
    }

    /**
     * @param nanosTimeout negative is interpreted as infinite wait
     */
    public DeadlockException(long nanosTimeout, Object attachment, boolean guilty,
                             Set<DeadlockInfo> set)
    {
        super(nanosTimeout, attachment);
        mGuilty = guilty;
        if (set == null) {
            set = Collections.emptySet();
        }
        mSet = set;
    }

    /**
     * @return true if caller helped caused the deadlock; false if caller might
     * be innocent
     */
    public boolean isGuilty() {
        return mGuilty;
    }

    /**
     * @return the set of lock requests which were in a deadlock
     */
    public Set<DeadlockInfo> deadlockSet() {
        return mSet;
    }

    @Override
    public String getMessage() {
        return getMessage(true);
    }

    /**
     * @return message without deadlock set info
     */
    public String shortMessage() {
        return getMessage(false);
    }

    private String getMessage(boolean full) {
        var b = new StringBuilder(super.getMessage());
        b.append("; caller ");
        if (mGuilty) {
            b.append("helped cause the deadlock");
        } else {
            b.append("might be innocent");
        }
        b.append('.');
        if (full && !mSet.isEmpty()) {
            b.append(" Deadlock set: ").append(mSet);
        }
        return b.toString();
    }
}
