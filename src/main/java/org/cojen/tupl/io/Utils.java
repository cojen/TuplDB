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

/**
 * Generic I/O utility methods.
 *
 * @author Brian S O'Neill
 */
public class Utils {
    protected Utils() {
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
        try {
            closer = new Thread() {
                public void run() {
                    try {
                        close(resource, cause);
                    } catch (IOException e2) {
                        // Ignore.
                    }
                }
            };
            closer.setDaemon(true);
            closer.start();
        } catch (Throwable e2) {
            closer = null;
        }

        if (closer == null) {
            try {
                close(resource, cause);
            } catch (IOException e2) {
                // Ignore.
            }
        } else {
            // Block up to one second.
            try {
                closer.join(1000);
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
