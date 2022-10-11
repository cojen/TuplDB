/*
 *  Copyright (C) 2022 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.remote;

import java.util.concurrent.TimeUnit;

import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Ordering;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class RemoteUtils {
    static byte toByte(TimeUnit unit) {
        switch (unit) {
        default: return 0;
        case MICROSECONDS: return 1;
        case MILLISECONDS: return 2;
        case SECONDS: return 3;
        case MINUTES: return 4;
        case HOURS: return 5;
        case DAYS: return 6;
        }
    }

    static TimeUnit toTimeUnit(byte b) {
        switch (b) {
        default: return TimeUnit.NANOSECONDS;
        case 1: return TimeUnit.MICROSECONDS;
        case 2: return TimeUnit.MILLISECONDS;
        case 3: return TimeUnit.SECONDS;
        case 4: return TimeUnit.MINUTES;
        case 5: return TimeUnit.HOURS;
        case 6: return TimeUnit.DAYS;
        }
    }

    static byte toByte(DurabilityMode mode) {
        switch (mode) {
        default: return 0;
        case NO_SYNC: return 1;
        case NO_FLUSH: return 2;
        case NO_REDO: return 3;
        }
    }

    static DurabilityMode toDurabilityMode(byte b) {
        switch (b) {
        default: return DurabilityMode.SYNC;
        case 1: return DurabilityMode.NO_SYNC;
        case 2: return DurabilityMode.NO_FLUSH;
        case 3: return DurabilityMode.NO_REDO;
        }
    }

    static byte toByte(LockMode mode) {
        switch (mode) {
        default: return 0;
        case REPEATABLE_READ: return 1;
        case READ_COMMITTED: return 2;
        case READ_UNCOMMITTED: return 3;
        case UNSAFE: return 4;
        }
    }

    static LockMode toLockMode(byte b) {
        switch (b) {
        default: return LockMode.UPGRADABLE_READ;
        case 1: return LockMode.REPEATABLE_READ;
        case 2: return LockMode.READ_COMMITTED;
        case 3: return LockMode.READ_UNCOMMITTED;
        case 4: return LockMode.UNSAFE;
        }
    }

    static byte toByte(LockResult result) {
        switch (result) {
        default: return 0;
        case INTERRUPTED: return 1;
        case TIMED_OUT_LOCK: return 2;
        case DEADLOCK: return 3;
        case ACQUIRED: return 4;
        case UPGRADED: return 5;
        case OWNED_SHARED: return 6;
        case OWNED_UPGRADABLE: return 7;
        case OWNED_EXCLUSIVE: return 8;
        case UNOWNED: return 9;
        }
    }

    static LockResult toLockResult(byte b) {
        switch (b) {
        default: return LockResult.ILLEGAL;
        case 1: return LockResult.INTERRUPTED;
        case 2: return LockResult.TIMED_OUT_LOCK;
        case 3: return LockResult.DEADLOCK;
        case 4: return LockResult.ACQUIRED;
        case 5: return LockResult.UPGRADED;
        case 6: return LockResult.OWNED_SHARED;
        case 7: return LockResult.OWNED_UPGRADABLE;
        case 8: return LockResult.OWNED_EXCLUSIVE;
        case 9: return LockResult.UNOWNED;
        }
    }

    static byte toByte(Ordering ordering) {
        if (ordering == Ordering.ASCENDING) {
            return 1;
        } else if (ordering == Ordering.DESCENDING) {
            return -1;
        } else {
            return 0;
        }
    }

    static Ordering toOrdering(byte b) {
        if (b == 1) {
            return Ordering.ASCENDING;
        } else if (b == -1) {
            return Ordering.DESCENDING;
        } else {
            return Ordering.UNSPECIFIED;
        }
    }
}
