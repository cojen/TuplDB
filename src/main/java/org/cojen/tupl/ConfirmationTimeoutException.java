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

import org.cojen.tupl.ext.ReplicationManager;

/**
 * Thrown by {@link ReplicationManager} when replication confirmation timed out.
 *
 * @author Brian S O'Neill
 */
public class ConfirmationTimeoutException extends ConfirmationFailureException {
    private static final long serialVersionUID = 1L;

    private final long mNanosTimeout;

    private TimeUnit mUnit;

    /**
     * @param nanosTimeout negative is interpreted as infinite wait
     */
    public ConfirmationTimeoutException(long nanosTimeout) {
        super((String) null);
        mNanosTimeout = nanosTimeout;
    }

    @Override
    public String getMessage() {
        return Utils.timeoutMessage(mNanosTimeout, this);
    }

    @Override
    public long getTimeout() {
        return getUnit().convert(mNanosTimeout, TimeUnit.NANOSECONDS);
    }

    @Override
    public TimeUnit getUnit() {
        TimeUnit unit = mUnit;
        if (unit != null) {
            return unit;
        }
        return mUnit = Utils.inferUnit(TimeUnit.NANOSECONDS, mNanosTimeout);
    }

    @Override
    boolean isRecoverable() {
        return true;
    }
}
