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

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.nio.ByteOrder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.cojen.tupl.CorruptDatabaseException;

/**
 * Generic data and I/O utility methods.
 *
 * @author Brian S O'Neill
 */
public class Utils {
    private static final VarHandle cShortArrayLEHandle;
    private static final VarHandle cShortArrayBEHandle;
    private static final VarHandle cIntArrayLEHandle;
    private static final VarHandle cIntArrayBEHandle;
    private static final VarHandle cLongArrayLEHandle;
    private static final VarHandle cLongArrayBEHandle;

    static {
        try {
            cShortArrayLEHandle = MethodHandles.byteArrayViewVarHandle
                (short[].class, ByteOrder.LITTLE_ENDIAN);
            cShortArrayBEHandle = MethodHandles.byteArrayViewVarHandle
                (short[].class, ByteOrder.BIG_ENDIAN);
            cIntArrayLEHandle = MethodHandles.byteArrayViewVarHandle
                (int[].class, ByteOrder.LITTLE_ENDIAN);
            cIntArrayBEHandle = MethodHandles.byteArrayViewVarHandle
                (int[].class, ByteOrder.BIG_ENDIAN);
            cLongArrayLEHandle = MethodHandles.byteArrayViewVarHandle
                (long[].class, ByteOrder.LITTLE_ENDIAN);
            cLongArrayBEHandle = MethodHandles.byteArrayViewVarHandle
                (long[].class, ByteOrder.BIG_ENDIAN);
        } catch (Throwable e) {
            throw new ExceptionInInitializerError();
        }
    }

    protected Utils() {
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
    public static void encodeShortBE(byte[] b, int offset, int v) {
        cShortArrayBEHandle.set(b, offset, (short) v);
    }

    /**
     * Encodes a 16-bit integer, in little-endian format.
     *
     * @param b encode destination
     * @param offset offset into byte array
     * @param v value to encode
     */
    public static void encodeShortLE(byte[] b, int offset, int v) {
        cShortArrayLEHandle.set(b, offset, (short) v);
    }

    /**
     * Encodes a 32-bit integer, in big-endian format.
     *
     * @param b encode destination
     * @param offset offset into byte array
     * @param v value to encode
     */
    public static void encodeIntBE(byte[] b, int offset, int v) {
        cIntArrayBEHandle.set(b, offset, v);
    }

    /**
     * Encodes a 32-bit integer, in little-endian format.
     *
     * @param b encode destination
     * @param offset offset into byte array
     * @param v value to encode
     */
    public static void encodeIntLE(byte[] b, int offset, int v) {
        cIntArrayLEHandle.set(b, offset, v);
    }

    /**
     * Encodes a 48-bit integer, in big-endian format.
     *
     * @param b encode destination
     * @param offset offset into byte array
     * @param v value to encode
     */
    public static void encodeInt48BE(byte[] b, int offset, long v) {
        encodeShortBE(b, offset, (int) (v >> 32));
        encodeIntBE(b, offset + 2, (int) v);
    }

    /**
     * Encodes a 48-bit integer, in little-endian format.
     *
     * @param b encode destination
     * @param offset offset into byte array
     * @param v value to encode
     */
    public static void encodeInt48LE(byte[] b, int offset, long v) {
        encodeIntLE(b, offset, (int) v);
        encodeShortLE(b, offset + 4, (int) (v >> 32));
    }

    /**
     * Encodes a 64-bit integer, in big-endian format.
     *
     * @param b encode destination
     * @param offset offset into byte array
     * @param v value to encode
     */
    public static void encodeLongBE(byte[] b, int offset, long v) {
        cLongArrayBEHandle.set(b, offset, v);
    }

    /**
     * Encodes a 64-bit integer, in little-endian format.
     *
     * @param b encode destination
     * @param offset offset into byte array
     * @param v value to encode
     */
    public static void encodeLongLE(byte[] b, int offset, long v) {
        cLongArrayLEHandle.set(b, offset, v);
    }

    /**
     * Decodes a 16-bit integer, in big-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static short decodeShortBE(byte[] b, int offset) {
        return (short) cShortArrayBEHandle.get(b, offset);
    }

    /**
     * Decodes a 16-bit integer, in little-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static short decodeShortLE(byte[] b, int offset) {
        return (short) cShortArrayLEHandle.get(b, offset);
    }

    /**
     * Decodes a 16-bit unsigned integer, in big-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static int decodeUnsignedShortBE(byte[] b, int offset) {
        return ((short) cShortArrayBEHandle.get(b, offset)) & 0xffff;
    }

    /**
     * Decodes a 16-bit unsigned integer, in little-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static int decodeUnsignedShortLE(byte[] b, int offset) {
        return ((short) cShortArrayLEHandle.get(b, offset)) & 0xffff;
    }

    /**
     * Decodes a 32-bit integer, in big-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static int decodeIntBE(byte[] b, int offset) {
        return (int) cIntArrayBEHandle.get(b, offset);
    }

    /**
     * Decodes a 32-bit integer, in little-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static int decodeIntLE(byte[] b, int offset) {
        return (int) cIntArrayLEHandle.get(b, offset);
    }

    /**
     * Decodes a 32-bit unsigned integer, in big-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static long decodeUnsignedIntBE(byte[] b, int offset) {
        return ((int) cIntArrayBEHandle.get(b, offset)) & 0xffff_ffffL;
    }

    /**
     * Decodes a 32-bit unsigned integer, in little-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static long decodeUnsignedIntLE(byte[] b, int offset) {
        return ((int) cIntArrayLEHandle.get(b, offset)) & 0xffff_ffffL;
    }

    /**
     * Decodes a 48-bit unsigned integer, in big-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static long decodeUnsignedInt48BE(byte[] b, int offset) {
        return (((long) decodeUnsignedShortBE(b, offset)) << 32)
            | decodeUnsignedIntBE(b, offset + 2);
    }

    /**
     * Decodes a 48-bit unsigned integer, in little-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static long decodeUnsignedInt48LE(byte[] b, int offset) {
        return decodeUnsignedIntLE(b, offset)
            | (((long) decodeUnsignedShortLE(b, offset + 4)) << 32);
    }

    /**
     * Decodes a 64-bit integer, in big-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static long decodeLongBE(byte[] b, int offset) {
        return (long) cLongArrayBEHandle.get(b, offset);
    }

    /**
     * Decodes a 64-bit integer, in little-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static long decodeLongLE(byte[] b, int offset) {
        return (long) cLongArrayLEHandle.get(b, offset);
    }

    /**
     * Fully reads the required length of bytes, throwing an EOFException if the end of stream
     * is reached too soon.
     */
    public static void readFully(InputStream in, byte[] b, int off, int len) throws IOException {
        if (len > 0) {
            doReadFully(in, b, off, len);
        }
    }

    private static void doReadFully(InputStream in, byte[] b, int off, int len)
        throws IOException
    {
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

        if (cause instanceof RuntimeException e) {
            throw e;
        }
        if (cause instanceof Error e) {
            throw e;
        }
        if (cause instanceof IOException e) {
            throw e;
        }

        throw new CorruptDatabaseException(cause);
    }

    /**
     * Closes a resource without throwing another exception.
     *
     * @param resource can be null
     */
    public static void closeQuietly(Closeable resource) {
        closeQuietly(null, resource);
    }

    /**
     * Closes a resource without throwing another exception.
     *
     * @param resource can be null
     * @param cause passed to resource if it implements {@link CauseCloseable}
     */
    public static void closeQuietly(Closeable resource, Throwable cause) {
        closeQuietly(null, resource, cause);
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
        if (resource instanceof CauseCloseable cc) {
            cc.close(cause);
        } else {
            resource.close();
        }
    }

    /**
     * Attempt to delete file, throwing an IOException if the delete failed and the file still
     * exists.
     *
     * @param file file to delete; can be null
     */
    public static void delete(File file) throws IOException {
        if (file != null && !file.delete() && file.exists()) {
            throw new IOException("Unable to delete file: " + file);
        }
    }

    /**
     * Add a suppressed exception without creating a circular reference or throwing a new
     * exception.
     *
     * @param target exception to receive suppressed exception; can be null
     * @param toSuppress exception to suppress and add to target; can be null
     */
    public static void suppress(final Throwable target, final Throwable toSuppress) {
        try {
            if (target == null || toSuppress == null) {
                return;
            }

            // Check if exceptions and causes intersect at all, returning if so.
            Throwable t = target;
            do {
                Throwable s = toSuppress;
                do {
                    if (t == s) {
                        return;
                    }
                    s = s.getCause();
                } while (s != null);
                t = t.getCause();
            } while (t != null);

            Throwable[] s1 = target.getSuppressed();
            Throwable[] s2 = toSuppress.getSuppressed();

            if (s1.length != 0 || s2.length != 0) {
                var all = new HashSet<Throwable>();
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
        throw Utils.<RuntimeException>castAndThrow(e);
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
        throw Utils.<RuntimeException>castAndThrow(e);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> RuntimeException castAndThrow(Throwable e) throws T {
        throw (T) e;
    }
}
