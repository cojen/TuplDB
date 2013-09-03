/*
 *  Copyright 2013 Brian S O'Neill
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

import org.cojen.tupl.CorruptDatabaseException;

import java.io.Closeable;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

/**
 * Generic data and I/O utility methods.
 *
 * @author Brian S O'Neill
 */
public class Utils {
    protected Utils() {
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
                    cCloseThreads = new HashMap<Closeable, Thread>(4);
                } else {
                    closer = cCloseThreads.get(resource);
                    if (closer != null) {
                        // First thread waited, which is sufficient.
                        joinMillis = 0;
                        break obtainThread;
                    }
                }

                closer = new Thread() {
                    public void run() {
                        try {
                            close(resource, cause);
                        } catch (IOException e2) {
                            // Ignore.
                        } finally {
                            unregister(resource);
                        }
                    }
                };

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
     * Returns the root cause of the given exception.
     *
     * @return non-null cause, unless given exception was null
     */
    public static Throwable rootCause(Throwable e) {
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
