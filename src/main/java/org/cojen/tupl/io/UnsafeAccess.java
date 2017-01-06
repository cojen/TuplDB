/*
 *  Copyright 2016 Cojen.org
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

package org.cojen.tupl.io;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

/**
 * Utility for accessing the unsupported Unsafe class.
 *
 * @author Brian S O'Neill
 */
public class UnsafeAccess {
    private static final Unsafe UNSAFE;
    private static final Throwable UNSUPPORTED;

    static {
        Unsafe unsafe = null;
        Throwable unsupported = null;

        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            unsafe = (Unsafe) theUnsafe.get(null);
        } catch (Throwable e) {
            unsupported = e;
        }

        UNSAFE = unsafe;
        UNSUPPORTED = unsupported;
    }

    /**
     * @return null if not supported
     */
    public static Unsafe tryObtain() {
        return UNSAFE;
    }

    /**
     * @throws UnsupportedOperationException if not supported
     */
    public static Unsafe obtain() throws UnsupportedOperationException {
        Unsafe u = UNSAFE;
        if (u == null) {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }
        return u;
    }
}
