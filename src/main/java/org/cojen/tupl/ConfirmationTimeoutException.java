/*
 *  Copyright 2013 Brian S O'Neill
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
