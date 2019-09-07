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
 * Provides a way to access non-public fields between packages.
 *
 * @author Brian S O'Neill
 */
public final class FriendAccess {
    private static HashMap<Object, VarHandle> cHandles;

    /**
     * To be called by the static initializer of the class which is providing access.
     */
    public static synchronized void register(MethodHandles.Lookup lookup,
                                             String varName, Class varType)
    {
        try {
            Class owner = lookup.lookupClass();
            VarHandle handle = lookup.findVarHandle(owner, varName, varType);
            if (cHandles == null) {
                cHandles = new HashMap<>();
            }
            cHandles.put(key(owner, varName), handle);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * @throws IllegalStateException if not found or if already consumed
     */
    static synchronized VarHandle consume(Class owner, String varName) {
        Object key = key(owner, varName);

        while (true) {
            VarHandle handle;
            if (cHandles != null && (handle = cHandles.remove(key)) != null) {
                if (cHandles.isEmpty()) {
                    cHandles = null;
                }
                return handle;
            }

            if (owner == null) {
                throw new IllegalStateException();
            }

            // Force the static initializer to run (once).
            try {
                Class.forName(owner.getName());
            } catch (ClassNotFoundException e) {
                throw Utils.rethrow(e);
            }

            owner = null;
        }
    }

    private static Object key(Class owner, String varName) {
        return List.of(owner, varName);
    }
}
