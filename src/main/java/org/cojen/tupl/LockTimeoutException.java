/*
 *  Copyright 2011-2015 Cojen.org
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

    private TimeUnit mUnit;

    /**
     * @param nanosTimeout negative is interpreted as infinite wait
     */
    public LockTimeoutException(long nanosTimeout) {
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
}
