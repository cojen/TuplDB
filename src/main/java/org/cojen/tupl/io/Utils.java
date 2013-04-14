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
 * 
 *
 * @author Brian S O'Neill
 */
public class Utils {
    protected Utils() {
    }

    public static IOException closeOnFailure(final Closeable c, final Throwable e)
        throws IOException
    {
        // Close in a separate thread, in case of deadlock.
        Thread closer;
        try {
            closer = new Thread() {
                public void run() {
                    try {
                        close(c, e);
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
                close(c, e);
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

        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        }
        if (e instanceof Error) {
            throw (Error) e;
        }
        if (e instanceof IOException) {
            throw (IOException) e;
        }

        throw new CorruptDatabaseException(e);
    }

    /**
     * @param first returned if non-null
     * @param c can be null
     * @return IOException which was caught, unless e was non-null
     */
    public static IOException closeQuietly(IOException first, Closeable c) {
        if (c != null) {
            try {
                c.close();
            } catch (IOException e) {
                if (first == null) {
                    return e;
                }
            }
        }
        return first;
    }

    /**
     * @param first returned if non-null
     * @param c can be null
     * @return IOException which was caught, unless e was non-null
     */
    public static IOException closeQuietly(IOException first, Closeable c, Throwable cause) {
        if (c != null) {
            try {
                close(c, cause);
            } catch (IOException e) {
                if (first == null) {
                    return e;
                }
            }
        }
        return first;
    }

    public static void close(Closeable c, Throwable cause) throws IOException {
        if (c instanceof CauseCloseable) {
            ((CauseCloseable) c).close(cause);
        } else {
            c.close();
        }
    }

    public static Throwable rootCause(Throwable e) {
        while (true) {
            Throwable cause = e.getCause();
            if (cause == null) {
                return e;
            }
            e = cause;
        }
    }

    public static void uncaught(Throwable e) {
        Thread t = Thread.currentThread();
        t.getUncaughtExceptionHandler().uncaughtException(t, e);
    }

    public static RuntimeException rethrow(Throwable e) {
        Utils.<RuntimeException>castAndThrow(e);
        return null;
    }

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
