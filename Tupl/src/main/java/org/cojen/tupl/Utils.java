/*
 *  Copyright 2011 Brian S O'Neill
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

package org.cojen.tupl;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import java.util.concurrent.TimeUnit;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class Utils {
    static final byte[] EMPTY_BYTES = new byte[0];

    static long toNanos(long timeout, TimeUnit unit) {
        return timeout < 0 ? -1 :
            (timeout == 0 ? 0 : (((timeout = unit.toNanos(timeout)) < 0) ? 0 : timeout));
    }

    static int roundUpPower2(int i) {
        // Hacker's Delight figure 3-3.
        i--;
        i |= i >> 1;
        i |= i >> 2;
        i |= i >> 4;
        i |= i >> 8;
        return (i | (i >> 16)) + 1;
    }

    /**
     * Performs multiple array copies, correctly ordered to prevent clobbering. The copies
     * must not overlap, and start1 must be less than start2.
     */
    static void arrayCopies(byte[] page,
                            int start1, int dest1, int length1,
                            int start2, int dest2, int length2)
    {
        if (dest1 < start1) {
            System.arraycopy(page, start1, page, dest1, length1);
            System.arraycopy(page, start2, page, dest2, length2);
        } else {
            System.arraycopy(page, start2, page, dest2, length2);
            System.arraycopy(page, start1, page, dest1, length1);
        }
    }

    /**
     * Performs multiple array copies, correctly ordered to prevent clobbering. The copies
     * must not overlap, start1 must be less than start2, and start2 be less than start3.
     */
    static void arrayCopies(byte[] page,
                            int start1, int dest1, int length1,
                            int start2, int dest2, int length2,
                            int start3, int dest3, int length3)
    {
        if (dest1 < start1) {
            System.arraycopy(page, start1, page, dest1, length1);
            arrayCopies(page, start2, dest2, length2, start3, dest3, length3);
        } else {
            arrayCopies(page, start2, dest2, length2, start3, dest3, length3);
            System.arraycopy(page, start1, page, dest1, length1);
        }
    }

    /**
     * @param a key 'a'
     * @param aoff key 'a' offset
     * @param alen key 'a' length
     * @param b key 'b'
     * @param boff key 'b' offset
     * @param blen key 'b' length
     * @return negative if 'a' is less, zero if equal, greater than zero if greater
     */
    static int compareKeys(byte[] a, int aoff, int alen, byte[] b, int boff, int blen) {
        int minLen = Math.min(alen, blen);
        for (int i=0; i<minLen; i++) {
            byte ab = a[aoff + i];
            byte bb = b[boff + i];
            if (ab != bb) {
                return ((ab & 0xff) < (bb & 0xff)) ? -1 : 1;
            }
        }
        return alen - blen;
    }

    /**
     * Deletes all files in the base file's directory which are named like
     * "base<pattern><number>". For example, mybase.redo.123
     */
    static void deleteNumberedFiles(File baseFile, String pattern) throws IOException {
        String prefix = baseFile.getName() + pattern;
        for (File file : baseFile.getParentFile().listFiles()) {
            String name = file.getName();
            if (name.startsWith(prefix)) {
                String suffix = name.substring(prefix.length());
                try {
                    Long.parseLong(suffix);
                } catch (NumberFormatException e) {
                    continue;
                }
                file.delete();
            }
        }
    }

    static IOException closeOnFailure(final Closeable c, Throwable e) throws IOException {
        // Close in a separate thread, in case of deadlock.
        Thread closer;
        try {
            closer = new Thread() {
                public void run() {
                    try {
                        c.close();
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
                c.close();
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
    static IOException closeQuietly(IOException first, Closeable c) {
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

    static void uncaught(Throwable e) {
        Thread t = Thread.currentThread();
        t.getUncaughtExceptionHandler().uncaughtException(t, e);
    }

    static RuntimeException rethrow(Throwable e) {
        Utils.<RuntimeException>castAndThrow(e);
        return null;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void castAndThrow(Throwable e) throws T {
        throw (T) e;
    }
}
