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

import java.util.concurrent.TimeUnit;

/**
 * Thrown when a lock request by a {@link Transaction transaction} timed out. A
 * {@link DatabaseConfig#lockTimeout default} timeout is defined, which can be
 * {@link Transaction#lockTimeout overridden} by a transaction.
 *
 * @author Brian S O'Neill
 * @see LockResult#TIMED_OUT_LOCK
 */
public class LockTimeoutException extends LockFailureException {
    private static final long serialVersionUID = 1L;

    private final long mNanosTimeout;
    private final Object mOwnerAttachment;

    private TimeUnit mUnit;

    /**
     * @param nanosTimeout negative is interpreted as infinite wait
     */
    public LockTimeoutException(long nanosTimeout) {
        this(nanosTimeout, null);
    }

    /**
     * @param nanosTimeout negative is interpreted as infinite wait
     */
    public LockTimeoutException(long nanosTimeout, Object attachment) {
        super((String) null);
        mNanosTimeout = nanosTimeout;
        mOwnerAttachment = attachment;
    }

    @Override
    public String getMessage() {
        return Utils.timeoutMessage(mNanosTimeout, this);
    }

    @Override
    public long getTimeout() {
        return getUnit().convert(mNanosTimeout, TimeUnit.NANOSECONDS);
    }

    /**
     * Returns the object which was {@link Transaction#attach attached} to the current lock
     * owner shortly after the timeout occurred. If an exclusive lock request failed because
     * any shared locks were held, only the first discovered attachment is provided.
     */
    @Override
    public Object getOwnerAttachment() {
        return mOwnerAttachment;
    }

    @Override
    public TimeUnit getUnit() {
        TimeUnit unit = mUnit;
        if (unit != null) {
            return unit;
        }
        return mUnit = Utils.inferUnit(TimeUnit.NANOSECONDS, mNanosTimeout);
    }
}
