/*
 *  Copyright 2019 Cojen.org
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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.util.HashMap;
import java.util.List;

import org.cojen.tupl.LockMode;
import org.cojen.tupl.VerificationObserver;

/**
 * All the friends needed by this package.
 *
 * @author Brian S O'Neill
 */
public final class Friends {
    private static final VarHandle failedHandle, repeatableHandle, noReadLockHandle;

    static {
        failedHandle = FriendAccess.consume(VerificationObserver.class, "failed");
        repeatableHandle = FriendAccess.consume(LockMode.class, "repeatable");
        noReadLockHandle = FriendAccess.consume(LockMode.class, "noReadLock");
    }

    /**
     * Sets the VerificationObserver.failed field.
     */
    static void failed(VerificationObserver obs, boolean b) {
        failedHandle.set(obs, b);
    }

    /**
     * Gets the VerificationObserver.failed field.
     */
    static boolean failed(VerificationObserver obs) {
        return (boolean) failedHandle.get(obs);
    }

    /**
     * Gets the LockMode.repeatable field.
     */
    static int repeatable(LockMode mode) {
        return (int) repeatableHandle.get(mode);
    }

    /**
     * Gets the LockMode.noReadLock field.
     */
    static boolean noReadLock(LockMode mode) {
        return (boolean) noReadLockHandle.get(mode);
    }
}
