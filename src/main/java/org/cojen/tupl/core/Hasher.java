/*
 *  Copyright (C) 2021 Cojen.org
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

package org.cojen.tupl.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.nio.ByteOrder;

/**
 * Simple and efficient hash algorithm based on x31.
 *
 * @author Brian S O'Neill
 */
abstract class Hasher {
    private static final VarHandle cShortArrayHandle;
    private static final VarHandle cIntArrayHandle;
    private static final VarHandle cLongArrayHandle;

    static {
        try {
            var order = ByteOrder.nativeOrder();
            cShortArrayHandle = MethodHandles.byteArrayViewVarHandle(short[].class, order);
            cIntArrayHandle = MethodHandles.byteArrayViewVarHandle(int[].class, order);
            cLongArrayHandle = MethodHandles.byteArrayViewVarHandle(long[].class, order);
        } catch (Throwable e) {
            throw new ExceptionInInitializerError();
        }
    }

    private Hasher() {
    }

    public static long hash(long hash, byte[] b) {
        return hash(hash, b, 0, b.length);
    }

    public static long hash(long hash, byte[] b, int off, int len) {
        int end = off + len - 8;
        for (; off <= end; off += 8) {
            hash = ((hash << 5) - hash) ^ (long) cLongArrayHandle.get(b, off);
        }
        end += 4;
        if (off <= end) {
            hash = ((hash << 5) - hash) ^ (int) cIntArrayHandle.get(b, off);
            off += 4;
        }
        end += 2;
        if (off <= end) {
            hash = ((hash << 5) - hash) ^ (short) cShortArrayHandle.get(b, off);
            off += 2;
        }
        end += 1;
        if (off <= end) {
            hash = ((hash << 5) - hash) ^ b[off];
        }
        // Hash is used by the LockManager, which depends on the upper and lower bits being
        // scrambled, so use a strong scramble method. The fibHash method is faster, but it
        // doesn't scramble the upper bits as well.
        hash = Utils.scramble(hash);
        return hash;
    }
}
