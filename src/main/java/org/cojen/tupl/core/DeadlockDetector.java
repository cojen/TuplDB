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

package org.cojen.tupl.core;

import java.lang.ref.Reference;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Set;

import org.cojen.tupl.Index;

import org.cojen.tupl.diag.DeadlockInfo;

import org.cojen.tupl.table.RowStore;

/**
 * Used internally by Locker. Only detects deadlocks caused by independent threads. A thread
 * "self deadlock" caused by separate lockers in the same thread is not detected. This is
 * because there is only one thread blocked.  The detector relies on multiple threads to be
 * blocked waiting on a lock.  Lockers aren't registered with any specific thread, and
 * therefore locks cannot be owned by threads. If this policy changes, then the detector could
 * see that the thread is self deadlocked.
 *
 * @author Brian S O'Neill
 */
final class DeadlockDetector extends HashMap<Locker, Boolean> {
    // Note: This code does not consider proper thread-safety and directly examines the
    // contents of locks and lockers. It never modifies anything, so it is relatively safe and
    // deadlocks are usually detectable. All involved threads had to acquire latches at some
    // point, which implies memory barriers.

    private final Locker mOrigin;
    private final LinkedHashMap<Lock, Boolean> mLocks;

    boolean mGuilty;

    DeadlockDetector(Locker locker, boolean gatherLocks) {
        mOrigin = locker;
        mLocks = gatherLocks ? new LinkedHashMap<>() : null;
    }

    /**
     * @param lockType type of lock requested; TYPE_SHARED, TYPE_UPGRADABLE, or TYPE_EXCLUSIVE
     */
    Set<DeadlockInfo> newDeadlockSet(int lockType) {
        if (mLocks == null || mLocks.isEmpty()) {
            return Collections.emptySet();
        }

        var infos = new DeadlockInfo[mLocks.size()];

        final LockManager manager = mOrigin.mManager;

        int i = 0;
        for (Lock lock : mLocks.keySet()) {
            Object attachment = lock.findOwnerAttachment(mOrigin, false, lockType);
            infos[i++] = newDeadlockInfo(manager, lock, attachment);
        }

        return new DeadlockInfoSet(infos);
    }

    static DeadlockInfo newDeadlockInfo(LockManager manager, Lock lock, Object attachment) {
        if (lock instanceof DetachedLock) {
            return new DetachedDeadlockInfo(lock.toString(), attachment);
        }

        var info = new CoreDeadlockInfo();

        info.mIndexId = lock.mIndexId;
        info.mAttachment = attachment;

        Index ix = manager.indexById(info.mIndexId);
        if (ix != null) {
            info.mIndexName = ix.name();
        }

        byte[] key = lock.mKey;
        if (key != null) {
            key = key.clone();
        }
        info.mKey = key;

        Reference<LocalDatabase> dbRef = manager.mDatabaseRef;
        if (dbRef != null) {
            LocalDatabase db = dbRef.get();
            if (db != null) {
                RowStore rs = db.tryRowStore();
                if (rs != null) {
                    info.mRow = rs.toRow(ix, key);
                }
            }
        }

        return info;
    }

    /**
     * @return true if a deadlock was found
     */
    boolean scan() {
        return scan(mOrigin);
    }

    /**
     * @return true if a deadlock was found
     */
    private boolean scan(Locker locker) {
        boolean found = false;

        outer: while (true) {
            Lock lock = locker.mWaitingFor;
            if (lock == null) {
                return found;
            }

            if (mLocks != null) {
                mLocks.put(lock, Boolean.TRUE);
            }

            if (put(locker, Boolean.TRUE) != null) {
                // Any graph edge flowing into the original locker indicates guilt.
                if (locker == mOrigin) {
                    mGuilty = true;
                }
                return true;
            }

            Locker owner = lock.mOwner;
            Object shared = lock.getSharedLocker();

            // If the owner is the locker, then it's trying to upgrade. It's waiting for
            // another locker to release the shared lock.
            if (owner != null && owner != locker) {
                if (shared == null) {
                    // Tail call.
                    locker = owner;
                    continue outer;
                }
                found |= scan(owner);
            }

            if (shared instanceof Locker) {
                // Tail call.
                locker = (Locker) shared;
                continue outer;
            }

            if (!(shared instanceof Lock.LockerHTEntry[] entries)) {
                return found;
            }

            for (int i=entries.length; --i>=0; ) {
                for (Lock.LockerHTEntry e = entries[i]; e != null; ) {
                    Lock.LockerHTEntry next = e.mNext;
                    if (i == 0 && next == null) {
                        // Tail call.
                        locker = e.mOwner;
                        continue outer;
                    }
                    found |= scan(e.mOwner);
                    e = next;
                }
            }

            return found;
        }
    }
}
