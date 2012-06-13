/*
 *  Copyright 2011-2012 Brian S O'Neill
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
 * Exception thrown when a lock request by a {@link Transaction transaction}
 * timed out. A {@link DatabaseConfig#lockTimeout default} timeout is defined,
 * which can be {@link Transaction#lockTimeout overridden} by a transaction.
 *
 * @author Brian S O'Neill
 * @see LockResult#TIMED_OUT_LOCK
 */
public class LockTimeoutException extends LockFailureException {
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
        if (mNanosTimeout == 0) {
            return "Never waited";
        } else if (mNanosTimeout < 0) {
            return "Infinite wait";
        } else {
            StringBuilder b = new StringBuilder("Waited ");
            appendTimeout(b, getTimeout(), getUnit());
            return b.toString();
        }
    }

    public long getTimeout() {
        return getUnit().convert(mNanosTimeout, TimeUnit.NANOSECONDS);
    }

    public TimeUnit getUnit() {
        TimeUnit unit = mUnit;
        if (unit != null) {
            return unit;
        }
        return mUnit = inferUnit(TimeUnit.NANOSECONDS, mNanosTimeout);
    }

    static TimeUnit inferUnit(TimeUnit unit, long value) {
        infer: {
            if (value == 0) break infer;
            if ((value - (value /= 1000) * 1000) != 0) break infer;
            unit = TimeUnit.MICROSECONDS;
            if ((value - (value /= 1000) * 1000) != 0) break infer;
            unit = TimeUnit.MILLISECONDS;
            if ((value - (value /= 1000) * 1000) != 0) break infer;
            unit = TimeUnit.SECONDS;
            if ((value - (value /= 60) * 60) != 0) break infer;
            unit = TimeUnit.MINUTES;
            if ((value - (value /= 60) * 60) != 0) break infer;
            unit = TimeUnit.HOURS;
            if ((value - (value / 24) * 24) != 0) break infer;
            unit = TimeUnit.DAYS;
        }

        return unit;
    }

    static void appendTimeout(StringBuilder b, long timeout, TimeUnit unit) {
        if (timeout == 0) {
            b.append('0');
        } else if (timeout < 0) {
            b.append("infinite");
        } else {
            b.append(timeout);
            b.append(' ');
            String unitStr = unit.toString().toLowerCase();
            if (timeout == 1) {
                unitStr = unitStr.substring(0, unitStr.length() - 1);
            }
            b.append(unitStr);
        }
    }
}
