/*
 *  Copyright 2015 Brian S O'Neill
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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * Backdoor access to DirectByteBuffer.
 *
 * @author Brian S O'Neill
 */
public class DirectAccess {
    private static final Field cDirectAddress;
    private static final Field cDirectCapacity;
    private static final Constructor<?> cDirectCtor;
    private static final ThreadLocal<ByteBuffer> cLocalBuffer;
    private static final ThreadLocal<ByteBuffer> cLocalBuffer2;

    static {
        Field addrField;
        Field capField;
        Constructor<?> ctor;
        ThreadLocal<ByteBuffer> local;
        ThreadLocal<ByteBuffer> local2;

        try {
            addrField = Buffer.class.getDeclaredField("address");
            addrField.setAccessible(true);

            capField = Buffer.class.getDeclaredField("capacity");
            capField.setAccessible(true);

            Class<?> clazz = Class.forName("java.nio.DirectByteBuffer");
            ctor = clazz.getDeclaredConstructor(long.class, int.class);
            ctor.setAccessible(true);

            local = new ThreadLocal<>();
            local2 = new ThreadLocal<>();
        } catch (Exception e) {
            addrField = null;
            capField = null;
            ctor = null;
            local = null;
            local2 = null;
        }

        cDirectAddress = addrField;
        cDirectCapacity = capField;
        cDirectCtor = ctor;
        cLocalBuffer = local;
        cLocalBuffer2 = local2;
    }

    /**
     * Returns a thread-local ByteBuffer which references any memory address. The position is
     * set to zero, the limit and capacity are set to the given length.
     *
     * @throws UnsupportedOperationException if not supported
     */
    public static ByteBuffer ref(long ptr, int length) {
        return ref(cLocalBuffer, ptr, length);
    }

    /**
     * Returns a second independent thread-local ByteBuffer.
     *
     * @throws UnsupportedOperationException if not supported
     */
    public static ByteBuffer ref2(long ptr, int length) {
        return ref(cLocalBuffer2, ptr, length);
    }

    private static ByteBuffer ref(ThreadLocal<ByteBuffer> local, long ptr, int length) {
        if (local == null) {
            throw new UnsupportedOperationException();
        }

        ByteBuffer bb = local.get();

        try {
            if (bb == null) {
                bb = (ByteBuffer) cDirectCtor.newInstance(ptr, length);
                local.set(bb);
            } else {
                cDirectAddress.setLong(bb, ptr);
                cDirectCapacity.setInt(bb, length);
            }
        } catch (Exception e) {
            throw new UnsupportedOperationException(e);
        }

        bb.position(0).limit(length);

        return bb;
    }

    public static void unref(ByteBuffer bb) {
        bb.position(0).limit(0);
        try {
            cDirectCapacity.setInt(bb, 0);
            cDirectAddress.setLong(bb, 0);
        } catch (Exception e) {
            throw new UnsupportedOperationException(e);
        }
    }
}
