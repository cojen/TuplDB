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

package org.cojen.tupl.io;

import org.cojen.tupl.CorruptDatabaseException;

import java.io.Closeable;
import java.io.EOFException;
import java.io.InputStream;
import java.io.IOException;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.nio.Buffer;
import java.nio.ByteBuffer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Generic data and I/O utility methods.
 *
 * @author Brian S O'Neill
 */
public class Utils {
    private static final MethodHandle cCompareUnsigned_1; // basic form
    private static final MethodHandle cCompareUnsigned_2; // offset/length form
    private static final MethodHandle cCompareUnsigned_3; // start/end form

    static {
        MethodType type = MethodType.methodType(int.class, byte[].class, byte[].class);
        MethodHandle method = findFastCompareMethod("compareUnsigned", type);
        if (method == null) {
            method = findLocalCompareMethod("doCompareUnsigned", type);
        }
        cCompareUnsigned_1 = method;

        type = MethodType.methodType
            (int.class, byte[].class, int.class, int.class, byte[].class, int.class, int.class);
        method = findFastCompareMethod("compareUnsigned", type);
        if (method == null) {
            cCompareUnsigned_2 = findLocalCompareMethod("doCompareUnsigned", type);
            cCompareUnsigned_3 = null; // won't be used
        } else {
            // Use an adapter to fix handling of length paramaters.
            cCompareUnsigned_2 = findLocalCompareMethod("compareUnsignedAdapter", type);
            cCompareUnsigned_3 = method;
        }
    }

    private static MethodHandle findFastCompareMethod(String name, MethodType type) {
        try {
            return MethodHandles.publicLookup().findStatic(Arrays.class, name, type);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            return null;
        }
    }

    private static MethodHandle findLocalCompareMethod(String name, MethodType type) {
        try {
            return MethodHandles.lookup().findStatic(Utils.class, name, type);
        } catch (Exception e2) {
            throw rethrow(e2);
        }
    }

    protected Utils() {
    }

    /**
     * Performs a lexicographical comparison between two unsigned byte arrays.
     *
     * @return negative if 'a' is less, zero if equal, greater than zero if greater
     */
    public static int compareUnsigned(byte[] a, byte[] b) {
        try {
            return (int) cCompareUnsigned_1.invokeExact(a, b);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    /**
     * Performs a lexicographical comparison between two unsigned byte arrays.
     *
     * @param a array 'a'
     * @param aoff array 'a' offset
     * @param alen array 'a' length
     * @param b array 'b'
     * @param boff array 'b' offset
     * @param blen array 'b' length
     * @return negative if 'a' is less, zero if equal, greater than zero if greater
     */
    public static int compareUnsigned(byte[] a, int aoff, int alen, byte[] b, int boff, int blen) {
        try {
            return (int) cCompareUnsigned_2.invokeExact(a, aoff, alen, b, boff, blen);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    @SuppressWarnings("unused")
    private static int doCompareUnsigned(byte[] a, byte[] b) {
        return doCompareUnsigned(a, 0, a.length, b, 0, b.length);
    }

    private static int doCompareUnsigned(byte[] a, int aoff, int alen,
                                         byte[] b, int boff, int blen)
    {
        int minLen = Math.min(alen, blen);
        for (int i=0; i<minLen; i++) {
            byte ab = a[aoff + i];
            byte bb = b[boff + i];
            if (ab != bb) {
                return (ab & 0xff) - (bb & 0xff);
            }
        }
        return alen - blen;
    }

    /**
     * Adapts the offset/length form to work with the start/end form.
     */
    @SuppressWarnings("unused")
    private static int compareUnsignedAdapter(byte[] a, int aoff, int alen,
                                              byte[] b, int boff, int blen)
        throws Throwable
    {
        return (int) cCompareUnsigned_3.invokeExact(a, aoff, aoff + alen, b, boff, boff + blen);
    }

    /**
     * Adds one to an unsigned integer, represented as a byte array. If
     * overflowed, value in byte array is 0x00, 0x00, 0x00...
     *
     * @param value unsigned integer to increment
     * @param start inclusive index
     * @param end exclusive index
     * @return false if overflowed
     */
    public static boolean increment(byte[] value, final int start, int end) {
        while (--end >= start) {
            if (++value[end] != 0) {
                // No carry bit, so done adding.
                return true;
            }
        }
        // This point is reached upon overflow.
        return false;
    }

    /**
     * Subtracts one from an unsigned integer, represented as a byte array. If
     * overflowed, value in byte array is 0xff, 0xff, 0xff...
     *
     * @param value unsigned integer to decrement
     * @param start inclusive index
     * @param end exclusive index
     * @return false if overflowed
     */
    public static boolean decrement(byte[] value, final int start, int end) {
        while (--end >= start) {
            if (--value[end] != -1) {
                // No borrow bit, so done subtracting.
                return true;
            }
        }
        // This point is reached upon overflow.
        return false;
    }

    /**
     * Encodes a 16-bit integer, in big-endian format.
     *
     * @param b encode destination
     * @param offset offset into byte array
     * @param v value to encode
     */
    public static final void encodeShortBE(byte[] b, int offset, int v) {
        b[offset    ] = (byte)(v >> 8);
        b[offset + 1] = (byte)v;
    }

    /**
     * Encodes a 16-bit integer, in little-endian format.
     *
     * @param b encode destination
     * @param offset offset into byte array
     * @param v value to encode
     */
    public static final void encodeShortLE(byte[] b, int offset, int v) {
        b[offset    ] = (byte)v;
        b[offset + 1] = (byte)(v >> 8);
    }

    /**
     * Encodes a 32-bit integer, in big-endian format.
     *
     * @param b encode destination
     * @param offset offset into byte array
     * @param v value to encode
     */
    public static final void encodeIntBE(byte[] b, int offset, int v) {
        b[offset    ] = (byte)(v >> 24);
        b[offset + 1] = (byte)(v >> 16);
        b[offset + 2] = (byte)(v >> 8);
        b[offset + 3] = (byte)v;
    }

    /**
     * Encodes a 32-bit integer, in little-endian format.
     *
     * @param b encode destination
     * @param offset offset into byte array
     * @param v value to encode
     */
    public static final void encodeIntLE(byte[] b, int offset, int v) {
        b[offset    ] = (byte)v;
        b[offset + 1] = (byte)(v >> 8);
        b[offset + 2] = (byte)(v >> 16);
        b[offset + 3] = (byte)(v >> 24);
    }

    /**
     * Encodes a 48-bit integer, in big-endian format.
     *
     * @param b encode destination
     * @param offset offset into byte array
     * @param v value to encode
     */
    public static final void encodeInt48BE(byte[] b, int offset, long v) {
        int w = (int)(v >> 32);
        b[offset    ] = (byte)(w >> 8);
        b[offset + 1] = (byte)w;
        w = (int)v;
        b[offset + 2] = (byte)(w >> 24);
        b[offset + 3] = (byte)(w >> 16);
        b[offset + 4] = (byte)(w >> 8);
        b[offset + 5] = (byte)w;
    }

    /**
     * Encodes a 48-bit integer, in little-endian format.
     *
     * @param b encode destination
     * @param offset offset into byte array
     * @param v value to encode
     */
    public static final void encodeInt48LE(byte[] b, int offset, long v) {
        int w = (int)v;
        b[offset    ] = (byte)w;
        b[offset + 1] = (byte)(w >> 8);
        b[offset + 2] = (byte)(w >> 16);
        b[offset + 3] = (byte)(w >> 24);
        w = (int)(v >> 32);
        b[offset + 4] = (byte)w;
        b[offset + 5] = (byte)(w >> 8);
    }

    /**
     * Encodes a 64-bit integer, in big-endian format.
     *
     * @param b encode destination
     * @param offset offset into byte array
     * @param v value to encode
     */
    public static final void encodeLongBE(byte[] b, int offset, long v) {
        int w = (int)(v >> 32);
        b[offset    ] = (byte)(w >> 24);
        b[offset + 1] = (byte)(w >> 16);
        b[offset + 2] = (byte)(w >> 8);
        b[offset + 3] = (byte)w;
        w = (int)v;
        b[offset + 4] = (byte)(w >> 24);
        b[offset + 5] = (byte)(w >> 16);
        b[offset + 6] = (byte)(w >> 8);
        b[offset + 7] = (byte)w;
    }

    /**
     * Encodes a 64-bit integer, in little-endian format.
     *
     * @param b encode destination
     * @param offset offset into byte array
     * @param v value to encode
     */
    public static final void encodeLongLE(byte[] b, int offset, long v) {
        int w = (int)v;
        b[offset    ] = (byte)w;
        b[offset + 1] = (byte)(w >> 8);
        b[offset + 2] = (byte)(w >> 16);
        b[offset + 3] = (byte)(w >> 24);
        w = (int)(v >> 32);
        b[offset + 4] = (byte)w;
        b[offset + 5] = (byte)(w >> 8);
        b[offset + 6] = (byte)(w >> 16);
        b[offset + 7] = (byte)(w >> 24);
    }

    /**
     * Decodes a 16-bit unsigned integer, in big-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static final int decodeUnsignedShortBE(byte[] b, int offset) {
        return ((b[offset] & 0xff) << 8) | ((b[offset + 1] & 0xff));
    }

    /**
     * Decodes a 16-bit unsigned integer, in little-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static final int decodeUnsignedShortLE(byte[] b, int offset) {
        return ((b[offset] & 0xff)) | ((b[offset + 1] & 0xff) << 8);
    }

    /**
     * Decodes a 32-bit integer, in big-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static final int decodeIntBE(byte[] b, int offset) {
        return (b[offset] << 24) | ((b[offset + 1] & 0xff) << 16) |
            ((b[offset + 2] & 0xff) << 8) | (b[offset + 3] & 0xff);
    }

    /**
     * Decodes a 32-bit integer, in little-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static final int decodeIntLE(byte[] b, int offset) {
        return (b[offset] & 0xff) | ((b[offset + 1] & 0xff) << 8) |
            ((b[offset + 2] & 0xff) << 16) | (b[offset + 3] << 24);
    }

    /**
     * Decodes a 48-bit unsigned integer, in big-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static final long decodeUnsignedInt48BE(byte[] b, int offset) {
        return
            (((long)(((b[offset    ] & 0xff) << 8 ) |
                     ((b[offset + 1] & 0xff)      ))              ) << 32) |
            (((long)(((b[offset + 2]       ) << 24) |
                     ((b[offset + 3] & 0xff) << 16) |
                     ((b[offset + 4] & 0xff) << 8 ) |
                     ((b[offset + 5] & 0xff)      )) & 0xffffffffL)      );
    }

    /**
     * Decodes a 48-bit unsigned integer, in little-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static final long decodeUnsignedInt48LE(byte[] b, int offset) {
        return
            (((long)(((b[offset    ] & 0xff)      ) |
                     ((b[offset + 1] & 0xff) << 8 ) |
                     ((b[offset + 2] & 0xff) << 16) |
                     ((b[offset + 3]       ) << 24)) & 0xffffffffL)      ) |
            (((long)(((b[offset + 4] & 0xff)      ) |
                     ((b[offset + 5] & 0xff) << 8 ))              ) << 32);
    }

    /**
     * Decodes a 64-bit integer, in big-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static final long decodeLongBE(byte[] b, int offset) {
        return
            (((long)(((b[offset    ]       ) << 24) |
                     ((b[offset + 1] & 0xff) << 16) |
                     ((b[offset + 2] & 0xff) << 8 ) |
                     ((b[offset + 3] & 0xff)      ))              ) << 32) |
            (((long)(((b[offset + 4]       ) << 24) |
                     ((b[offset + 5] & 0xff) << 16) |
                     ((b[offset + 6] & 0xff) << 8 ) |
                     ((b[offset + 7] & 0xff)      )) & 0xffffffffL)      );
    }

    /**
     * Decodes a 64-bit integer, in little-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static final long decodeLongLE(byte[] b, int offset) {
        return
            (((long)(((b[offset    ] & 0xff)      ) |
                     ((b[offset + 1] & 0xff) << 8 ) |
                     ((b[offset + 2] & 0xff) << 16) |
                     ((b[offset + 3]       ) << 24)) & 0xffffffffL)      ) |
            (((long)(((b[offset + 4] & 0xff)      ) |
                     ((b[offset + 5] & 0xff) << 8 ) |
                     ((b[offset + 6] & 0xff) << 16) |
                     ((b[offset + 7]       ) << 24))              ) << 32);
    }

    /**
     * Fully reads the required length of bytes, throwing an EOFException if the end of stream
     * is reached too soon.
     */
    public static void readFully(InputStream in, byte[] b, int off, int len) throws IOException {
        if (len > 0) {
            while (true) {
                int amt = in.read(b, off, len);
                if (amt <= 0) {
                    throw new EOFException();
                }
                if ((len -= amt) <= 0) {
                    break;
                }
                off += amt;
            }
        }
    }

    private static volatile int cDeleteSupport;

    /**
     * Attempt to delete the given direct or mapped byte buffer.
     */
    public static boolean delete(Buffer bb) {
        return bb instanceof ByteBuffer ? delete((ByteBuffer) bb) : false;
    }

    /**
     * Attempt to delete the given direct or mapped byte buffer.
     */
    @SuppressWarnings("restriction")
    public static boolean delete(ByteBuffer bb) {
        if (!bb.isDirect()) {
            return false;
        }

        // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038

        int deleteSupport = cDeleteSupport;

        if (deleteSupport < 0) {
            return false;
        }

        if (deleteSupport == 0) {
            try {
                Method m = bb.getClass().getMethod("cleaner");
                m.setAccessible(true);
                Object cleaner = m.invoke(bb);
                if (cleaner == null) {
                    // No cleaner, so nothing to do.
                    return false;
                }
                m = cleaner.getClass().getMethod("clean");
                m.setAccessible(true);
                m.invoke(cleaner);
                return true;
            } catch (Exception e) {
                // Try another way.
                cDeleteSupport = 1;
            }
        }

        try {
            sun.misc.Unsafe u = UnsafeAccess.obtain();
            Method m = u.getClass().getMethod("invokeCleaner", ByteBuffer.class);
            m.invoke(u, bb);
            return true;
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IllegalArgumentException) {
                // Duplicate or slice.
                return false;
            }
            throw rethrow(cause);
        } catch (Exception e) {
            // Unsupported.
            cDeleteSupport = -1;
            return false;
        }
    }

    private static Map<Closeable, Thread> cCloseThreads;

    static synchronized void unregister(Closeable resource) {
        if (cCloseThreads != null) {
            cCloseThreads.remove(resource);
            if (cCloseThreads.isEmpty()) {
                cCloseThreads = null;
            }
        }
    }

    /**
     * Closes the given resource, passing the cause if the resource implements {@link
     * CauseCloseable}. The cause is then rethrown, wrapped by {@link CorruptDatabaseException}
     * if not an {@link IOException} or unchecked.
     */
    public static IOException closeOnFailure(final Closeable resource, final Throwable cause)
        throws IOException
    {
        // Close in a separate thread, in case of deadlock.
        Thread closer;
        int joinMillis;
        obtainThread: try {
            synchronized (Utils.class) {
                if (cCloseThreads == null) {
                    cCloseThreads = new HashMap<>(4);
                } else {
                    closer = cCloseThreads.get(resource);
                    if (closer != null) {
                        // First thread waited, which is sufficient.
                        joinMillis = 0;
                        break obtainThread;
                    }
                }

                closer = new Thread(() -> {
                    try {
                        close(resource, cause);
                    } catch (IOException e2) {
                        // Ignore.
                    } finally {
                        unregister(resource);
                    }
                });

                cCloseThreads.put(resource, closer);
            }

            closer.setDaemon(true);
            closer.start();

            // Wait up to one second for close to finish.
            joinMillis = 1000;
        } catch (Throwable e2) {
            closer = null;
            joinMillis = 0;
        }

        if (closer == null) {
            try {
                close(resource, cause);
            } catch (IOException e2) {
                // Ignore.
            } finally {
                unregister(resource);
            }
        } else if (joinMillis > 0) {
            try {
                closer.join(joinMillis);
            } catch (InterruptedException e2) {
            }
        }

        if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        if (cause instanceof IOException) {
            throw (IOException) cause;
        }

        throw new CorruptDatabaseException(cause);
    }

    /**
     * Closes a resource without throwing another exception. If closing a chain of resources,
     * pass in the first caught exception, and all others are discarded.
     *
     * @param first returned if non-null
     * @param resource can be null
     * @return IOException which was caught, unless first was non-null
     */
    public static IOException closeQuietly(IOException first, Closeable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (IOException e) {
                if (first == null) {
                    return e;
                }
            }
        }
        return first;
    }

    /**
     * Closes a resource without throwing another exception. If closing a chain of resources,
     * pass in the first caught exception, and all others are discarded.
     *
     * @param first returned if non-null
     * @param resource can be null
     * @param cause passed to resource if it implements {@link CauseCloseable}
     * @return IOException which was caught, unless first was non-null
     */
    public static IOException closeQuietly(IOException first, Closeable resource, Throwable cause)
    {
        if (resource != null) {
            try {
                close(resource, cause);
            } catch (IOException e) {
                if (first == null) {
                    return e;
                }
            }
        }
        return first;
    }

    /**
     * Closes a resource, which may throw a new exception.
     *
     * @param cause passed to resource if it implements {@link CauseCloseable}
     */
    public static void close(Closeable resource, Throwable cause) throws IOException {
        if (resource instanceof CauseCloseable) {
            ((CauseCloseable) resource).close(cause);
        } else {
            resource.close();
        }
    }

    /**
     * Add a suppressed exception without creating a circular reference or throwing a new
     * exception.
     *
     * @param target exception to receive suppressed exception; can be null
     * @param toSuppress exception to suppress and add to target; can be null
     */
    public static void suppress(Throwable target, Throwable toSuppress) {
        try {
            if (target == null || toSuppress == null || target == toSuppress) {
                return;
            }

            // TODO: Should examine the entire cause chain in search of duplicates.
            if (target.getCause() == toSuppress || toSuppress.getCause() == target) {
                return;
            }

            Throwable[] s1 = target.getSuppressed();
            Throwable[] s2 = toSuppress.getSuppressed();

            if (s1.length != 0 || s2.length != 0) {
                Set<Throwable> all = new HashSet<>();
                all.add(target);
                if (!gatherSuppressed(all, s1) || !gatherSuppressed(all, s2)) {
                    return;
                }
                if (all.contains(toSuppress)) {
                    return;
                }
            }

            target.addSuppressed(toSuppress);
        } catch (Throwable e2) {
            // Ignore.
        }
    }

    /**
     * @return false if duplicates found
     */
    private static boolean gatherSuppressed(Set<Throwable> all, Throwable[] suppressed) {
        for (Throwable s : suppressed) {
            if (!gatherSuppressed(all, s)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return false if duplicates found
     */
    private static boolean gatherSuppressed(Set<Throwable> all, Throwable e) {
        if (!all.add(e)) {
            return false;
        }
        for (Throwable s : e.getSuppressed()) {
            if (!gatherSuppressed(all, s)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the root cause of the given exception.
     *
     * @return non-null cause, unless given exception was null
     */
    public static Throwable rootCause(Throwable e) {
        if (e == null) {
            return null;
        }
        while (true) {
            Throwable cause = e.getCause();
            if (cause == null) {
                return e;
            }
            e = cause;
        }
    }

    /**
     * Convenience method to pass the given exception to the current thread's uncaught
     * exception handler.
     */
    public static void uncaught(Throwable e) {
        Thread t = Thread.currentThread();
        t.getUncaughtExceptionHandler().uncaughtException(t, e);
    }

    /**
     * Rethrows the given exception without the compiler complaining about it being checked or
     * not. Use as follows: {@code throw rethrow(e)}
     */
    public static RuntimeException rethrow(Throwable e) {
        Utils.<RuntimeException>castAndThrow(e);
        return null;
    }

    /**
     * Rethrows the given exception without the compiler complaining about it being checked or
     * not. The exception can have a root cause initialized, which will be the root cause of
     * the one given. Use as follows: {@code throw rethrow(e, cause)}
     *
     * @param cause initialize the exception's cause, unless it already has one
     */
    public static RuntimeException rethrow(Throwable e, Throwable cause) {
        if (cause != null && e != cause && e.getCause() == null) {
            try {
                e.initCause(rootCause(cause));
            } catch (Exception e2) {
            } 
        }
        Utils.<RuntimeException>castAndThrow(e);
        return null;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void castAndThrow(Throwable e) throws T {
        throw (T) e;
    }
}
