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
    private static final long serialVersionUID = 1L;

    private final boolean mGuilty;
    private final DeadlockSet mSet;

    DeadlockException(long nanosTimeout, boolean guilty, DeadlockSet set) {
        super(nanosTimeout);
        mGuilty = guilty;
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
    public DeadlockSet getDeadlockSet() {
        return mSet;
    }

    @Override
    public String getMessage() {
        return getMessage(true);
    }

    /**
     * @return message without deadlock set info
     */
    public String getShortMessage() {
        return getMessage(false);
    }

    private String getMessage(boolean full) {
        StringBuilder b = new StringBuilder(super.getMessage());
        b.append("; caller ");
        if (mGuilty) {
            b.append("helped cause the deadlock");
        } else {
            b.append("might be innocent");
        }
        b.append('.');
        if (full) {
            b.append(" Deadlock set: ");
            mSet.appendMembers(b);
        }
        return b.toString();
    }
}
