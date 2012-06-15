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

/**
 * Thrown when a lock request by a {@link Transaction transaction} timed out
 * due to a deadlock. Deadlocks can be prevented by locking records in a
 * consistent order. Cases of "self deadlock" when using multiple transactions
 * in one thread are not detected, and a regular timeout exception is thrown
 * instead.
 *
 * @author Brian S O'Neill
 */
public class DeadlockException extends LockTimeoutException {
    private final boolean mGuilty;

    DeadlockException(long nanosTimeout, boolean guilty) {
        super(nanosTimeout);
        mGuilty = guilty;
    }

    public boolean isGuilty() {
        return mGuilty;
    }

    @Override
    public String getMessage() {
        String message;
        if (mGuilty) {
            message = "caller helped cause the deadlock";
        } else {
            message = "caller might be innocent";
        }
        return super.getMessage() + "; " + message;
    }
}
