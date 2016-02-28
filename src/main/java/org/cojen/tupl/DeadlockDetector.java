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

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Used internally by Locker. Only detects deadlocks caused by independent
 * threads. A thread "self deadlock" caused by separate lockers in the same
 * thread is not detected. This is because there is only one thread blocked.
 * The detector relies on multiple threads to be blocked waiting on a lock.
 * Lockers aren't registered with any specific thread, and therefore locks
 * cannot be owned by threads. If this policy changes, then the detector could
 * see that the thread is self deadlocked.
 *
 * @author Brian S O'Neill
 */
/*P*/
final class DeadlockDetector {
    // Note: This code does not consider proper thread-safety and directly
    // examines the contents of locks and lockers. It never modifies anything,
    // so it is relatively safe and deadlocks are usually detectable. All
    // involved threads had to acquire latches at some point, which implies a
    // memory barrier.

    private final Locker mOrigin;
    private final Set<LockOwner> mLockers;
    final Set<Lock> mLocks;

    boolean mGuilty;

    DeadlockDetector(Locker locker) {
        mOrigin = locker;
        mLockers = new LinkedHashSet<>();
        mLocks = new LinkedHashSet<>();
    }

    DeadlockSet newDeadlockSet() {
        int size = mLocks.size();
        long[] indexIds = new long[size];
        byte[][] keys = new byte[size][];

        int i = 0;
        for (Lock lock : mLocks) {
            indexIds[i] = lock.mIndexId;
            byte[] key = lock.mKey;
            if (key != null) {
                key = key.clone();
            }
            keys[i] = key;
            i++;
        }

        return new DeadlockSet(indexIds, keys);
    }

    /**
     * @return true if deadlock was found
     */
    boolean scan() {
        return scan(mOrigin);
    }

    /**
     * @return true if deadlock was found
     */
    private boolean scan(LockOwner locker) {
        boolean found = false;

        outer: while (true) {
            Lock lock = locker.mWaitingFor;
            if (lock == null) {
                return found;
            }

            mLocks.add(lock);

            if (mLockers.isEmpty()) {
                mLockers.add(locker);
            } else {
                // Any graph edge flowing into the original locker indicates guilt.
                mGuilty |= mOrigin == locker;
                if (!mLockers.add(locker)) {
                    return true;
                }
            }

            LockOwner owner = lock.mOwner;
            Object shared = lock.mSharedLockOwnersObj;

            // If the owner is the locker, then it is trying to upgrade. It's
            // waiting for another locker to release the shared lock.
            if (owner != null && owner != locker) {
                if (shared == null) {
                    // Tail call.
                    locker = owner;
                    continue outer;
                }
                found |= scan(owner);
            }

            scanShared: if (shared != null) {
                if (shared instanceof Locker) {
                    // Tail call.
                    locker = (Locker) shared;
                    continue outer;
                }

                if (!(shared instanceof Lock.LockOwnerHTEntry[])) {
                    break scanShared;
                }

                Lock.LockOwnerHTEntry[] entries = (Lock.LockOwnerHTEntry[]) shared;
                for (int i=entries.length; --i>=0; ) {
                    for (Lock.LockOwnerHTEntry e = entries[i]; e != null; ) {
                        Lock.LockOwnerHTEntry next = e.mNext;
                        if (i == 0 && next == null) {
                            // Tail call.
                            locker = e.mOwner;
                            continue outer;
                        }
                        found |= scan(e.mOwner);
                        e = next;
                    }
                }
            }

            return found;
        }
    }
}
