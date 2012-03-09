/*
 *  Copyright 2011 Brian S O'Neill
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
 * timed out. The timeout can be specified {@link DatabaseConfig#lockTimeout
 * globally} or per {@link Transaction#lockTimeout transaction}.
 *
 * @author Brian S O'Neill
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
        }
        if (mNanosTimeout < 0) {
            return "Infinite wait";
        }

        String unitStr = getUnit().toString().toLowerCase();
        long timeout = getTimeout();
        if (timeout == 1) {
            unitStr = unitStr.substring(0, unitStr.length() - 1);
        }

        return "Waited " + timeout + ' ' + unitStr;
    }

    public long getTimeout() {
        return getUnit().convert(mNanosTimeout, TimeUnit.NANOSECONDS);
    }

    public TimeUnit getUnit() {
        TimeUnit unit = mUnit;
        if (unit != null) {
            return unit;
        }

        unit = TimeUnit.NANOSECONDS;
        long value = mNanosTimeout;

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
            if ((value - (value /= 24) * 24) != 0) break infer;
            unit = TimeUnit.DAYS;
        }

        return mUnit = unit;
    }
}
