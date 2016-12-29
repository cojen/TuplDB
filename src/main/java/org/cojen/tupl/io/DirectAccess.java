/*
 *  Copyright 2015 Cojen.org
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

import java.nio.Buffer;
import java.nio.ByteBuffer;

import sun.misc.Unsafe;

/**
 * Backdoor access to DirectByteBuffer.
 *
 * @author Brian S O'Neill
 */
public class DirectAccess {
    private static final Unsafe UNSAFE = UnsafeAccess.tryObtain();

    private static final Class<?> cDirectByteBufferClass;
    private static final long cDirectAddressOffset;
    private static final long cDirectCapacityOffset;
    private static final ThreadLocal<ByteBuffer> cLocalBuffer;
    private static final ThreadLocal<ByteBuffer> cLocalBuffer2;

    static {
        Class<?> clazz;
        long addrOffset, capOffset;
        ThreadLocal<ByteBuffer> local;
        ThreadLocal<ByteBuffer> local2;

        try {
            clazz = Class.forName("java.nio.DirectByteBuffer");

            addrOffset = UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("address"));
            capOffset = UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("capacity"));

            local = new ThreadLocal<>();
            local2 = new ThreadLocal<>();
        } catch (Exception e) {
            clazz = null;
            addrOffset = 0;
            capOffset = 0;
            local = null;
            local2 = null;
        }

        cDirectByteBufferClass = clazz;
        cDirectAddressOffset = addrOffset;
        cDirectCapacityOffset = capOffset;
        cLocalBuffer = local;
        cLocalBuffer2 = local2;
    }

    private final ThreadLocal<ByteBuffer> mLocalBuffer;

    /**
     * @throws UnsupportedOperationException if not supported
     */
    public DirectAccess() {
        if (!isSupported()) {
            throw new UnsupportedOperationException();
        }
        mLocalBuffer = new ThreadLocal<>();
    }

    /**
     * Returns an instance-specific thread-local ByteBuffer which references any memory
     * address. The position is set to zero, the limit and capacity are set to the given
     * length.
     *
     * @throws UnsupportedOperationException if not supported
     */
    public ByteBuffer prepare(long ptr, int length) {
        return ref(mLocalBuffer, ptr, length);
    }

    public static boolean isSupported() {
        return cLocalBuffer2 != null;
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

    public static long getAddress(Buffer buf) {
        if (!buf.isDirect()) {
            throw new IllegalArgumentException("Not a direct buffer");
        }
        try {
            return UNSAFE.getLong(buf, cDirectAddressOffset);
        } catch (Exception e) {
            throw new UnsupportedOperationException(e);
        }
    }

    private static ByteBuffer ref(ThreadLocal<ByteBuffer> local, long ptr, int length) {
        if (local == null) {
            throw new UnsupportedOperationException();
        }

        ByteBuffer bb = local.get();

        try {
            if (bb == null) {
                bb = (ByteBuffer) UNSAFE.allocateInstance(cDirectByteBufferClass);
                bb.clear();
                local.set(bb);
            }
            UNSAFE.putLong(bb, cDirectAddressOffset, ptr);
            UNSAFE.putInt(bb, cDirectCapacityOffset, length);
        } catch (Exception e) {
            throw new UnsupportedOperationException(e);
        }

        bb.position(0).limit(length);

        return bb;
    }

    /**
     * Optionally unreference a buffer. The garbage collector does not attempt to free memory
     * referenced by a ByteBuffer created by this class.
     */
    public static void unref(ByteBuffer bb) {
        bb.position(0).limit(0);
        try {
            UNSAFE.putInt(bb, cDirectCapacityOffset, 0);
            UNSAFE.putLong(bb, cDirectAddressOffset, 0);
        } catch (Exception e) {
            throw new UnsupportedOperationException(e);
        }
    }
}
