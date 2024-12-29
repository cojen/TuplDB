/*
 *  Copyright (C) 2019 Cojen.org
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

package org.cojen.tupl.util;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.lang.ref.WeakReference;

import java.util.concurrent.locks.LockSupport;

import org.cojen.tupl.io.Utils;

/**
 * Alternative to directly using LockSupport for parking and unparking threads, which is much
 * faster when threads are parked briefly.
 *
 * @author Brian S O'Neill
 */
public abstract class Parker {
    private static final Parker PARKER;

    static {
        // Reduce the risk of "lost unpark" due to classloading.
        // https://bugs.openjdk.java.net/browse/JDK-8074773
        Class<?> clazz = LockSupport.class;

        String type = System.getProperty(Parker.class.getName());

        if (type == null) {
            PARKER = new Checked();
        } else {
            try {
                PARKER = (Parker) Class.forName(Parker.class.getName() + '$' + type)
                    .getDeclaredConstructor().newInstance();
            } catch (Throwable e) {
                throw Utils.rethrow(Utils.rootCause(e));
            }
        }
    }

    public static void unpark(Thread thread) {
        if (thread != null) {
            PARKER.doUnpark(thread);
        }
    }

    /**
     * Park after checking while spinning and yielding, to avoid putting the thread to sleep.
     */
    public static void park(Object blocker) {
        PARKER.doPark(blocker);
    }

    /**
     * Park without performing more checks than necessary. Should be used when caller has
     * already performed checks to determine that parking is necessary.
     */
    public static void parkNow(Object blocker) {
        PARKER.doParkNow(blocker);
    }

    /**
     * Park after checking while spinning and yielding, to avoid putting the thread to sleep.
     */
    public static void parkNanos(Object blocker, long nanos) {
        PARKER.doParkNanos(blocker, nanos);
    }

    /**
     * Park without performing more checks than necessary. Should be used when caller has
     * already performed checks to determine that parking is necessary.
     */
    public static void parkNanosNow(Object blocker, long nanos) {
        PARKER.doParkNanosNow(blocker, nanos);
    }

    Parker() {
    }

    abstract void doUnpark(Thread thread);

    abstract void doPark(Object blocker);

    abstract void doParkNow(Object blocker);

    abstract void doParkNanos(Object blocker, long nanos);

    abstract void doParkNanosNow(Object blocker, long nanos);

    private static final class Now extends Parker {
        @Override
        void doUnpark(Thread thread) {
            LockSupport.unpark(thread);
        }

        @Override
        void doPark(Object blocker) {
            LockSupport.park(blocker);
        }

        @Override
        void doParkNow(Object blocker) {
            LockSupport.park(blocker);
        }

        @Override
        void doParkNanos(Object blocker, long nanos) {
            LockSupport.parkNanos(blocker, nanos);
        }

        @Override
        void doParkNanosNow(Object blocker, long nanos) {
            LockSupport.parkNanos(blocker, nanos);
        }
    }

    private static final class Never extends Parker {
        @Override
        void doUnpark(Thread thread) {
        }

        @Override
        void doPark(Object blocker) {
        }

        @Override
        void doParkNow(Object blocker) {
        }

        @Override
        void doParkNanos(Object blocker, long nanos) {
        }

        @Override
        void doParkNanosNow(Object blocker, long nanos) {
        }
    }

    private static final class Spin extends Parker {
        @Override
        void doUnpark(Thread thread) {
        }

        @Override
        void doPark(Object blocker) {
            Thread.onSpinWait();
        }

        @Override
        void doParkNow(Object blocker) {
            Thread.onSpinWait();
        }

        @Override
        void doParkNanos(Object blocker, long nanos) {
            Thread.onSpinWait();
        }

        @Override
        void doParkNanosNow(Object blocker, long nanos) {
            Thread.onSpinWait();
        }
    }

    private static final class Yield extends Parker {
        @Override
        void doUnpark(Thread thread) {
        }

        @Override
        void doPark(Object blocker) {
            Thread.yield();
        }

        @Override
        void doParkNow(Object blocker) {
            Thread.yield();
        }

        @Override
        void doParkNanos(Object blocker, long nanos) {
            Thread.yield();
        }

        @Override
        void doParkNanosNow(Object blocker, long nanos) {
            Thread.yield();
        }
    }

    private static final class Checked extends Parker {
        private static final VarHandle STATE_HANDLE;

        static {
            try {
                var lookup = MethodHandles.lookup();
                STATE_HANDLE = lookup.findVarHandle(Entry.class, "mState", int.class);
            } catch (Throwable e) {
                throw Utils.rethrow(e);
            }
        }

        private static final long MAX_CHECK_NANOS = 10_000; // 10μs

        private static final int NONE = 0, PARKED = 1, UNPARKED = 2;

        private Entry[] mEntries;
        private int mSize;

        Checked() {
            mEntries = new Entry[16]; // power of 2
        }

        @Override
        void doUnpark(Thread thread) {
            if (!thread.isVirtual()) try {
                if (((int) STATE_HANDLE.getAndSet(entryFor(thread), UNPARKED)) != PARKED) {
                    return;
                }
            } catch (Throwable ex) {
                // Possibly an OutOfMemoryError. Always unpark to be safe.
            }
            LockSupport.unpark(thread);
        }

        @Override
        void doPark(Object blocker) {
            Thread thread = Thread.currentThread();
            if (thread.isVirtual()) {
                LockSupport.park(blocker);
            } else {
                Entry e = check(thread);
                if (e != null) {
                    LockSupport.park(blocker);
                    e.mState = NONE;
                }
            }
        }

        @Override
        void doParkNow(Object blocker) {
            Thread thread = Thread.currentThread();
            if (thread.isVirtual()) {
                LockSupport.park(blocker);
            } else {
                Entry e = checkNow(thread);
                if (e != null) {
                    LockSupport.park(blocker);
                    e.mState = NONE;
                }
            }
        }

        @Override
        void doParkNanos(Object blocker, long nanos) {
            Thread thread = Thread.currentThread();
            if (thread.isVirtual()) {
                LockSupport.parkNanos(blocker, nanos);
            } else {
                Entry e = check(thread);
                if (e != null) {
                    nanos -= MAX_CHECK_NANOS;
                    if (nanos > 0) {
                        LockSupport.parkNanos(blocker, nanos);
                    }
                    e.mState = NONE;
                }
            }
        }

        @Override
        void doParkNanosNow(Object blocker, long nanos) {
            Thread thread = Thread.currentThread();
            if (thread.isVirtual()) {
                LockSupport.parkNanos(blocker, nanos);
            } else {
                Entry e = checkNow(thread);
                if (e != null) {
                    nanos -= MAX_CHECK_NANOS;
                    if (nanos > 0) {
                        LockSupport.parkNanos(blocker, nanos);
                    }
                    e.mState = NONE;
                }
            }
        }

        /**
         * @return null if no need to actually park
         */
        private Entry check(final Thread thread) {
            final long start = System.nanoTime();

            Entry e;
            try {
                e = entryFor(thread);
            } catch (Throwable ex) {
                // Possibly an OutOfMemoryError. Be safe and don't park.
                return null;
            }

            int i = Latch.SPIN_LIMIT;
            while (true) {
                if (e.mState == UNPARKED) {
                    break;
                }
                if (thread.isInterrupted()) {
                    return null;
                }
                if (--i <= 0) {
                    Thread.yield();
                    i = Latch.SPIN_LIMIT;
                } else {
                    Thread.onSpinWait();
                }
                if ((System.nanoTime() - start) > MAX_CHECK_NANOS) {
                    if (STATE_HANDLE.compareAndSet(e, NONE, PARKED)) {
                        return e;
                    }
                    break;
                }
            }

            e.mState = NONE;
            return null;
        }

        /**
         * @return null if no need to actually park
         */
        private Entry checkNow(final Thread thread) {
            Entry e;
            try {
                e = entryFor(thread);
            } catch (Throwable ex) {
                // Possibly an OutOfMemoryError. Be safe and don't park.
                return null;
            }

            if (e.mState != UNPARKED) {
                if (thread.isInterrupted()) {
                    return null;
                }
                if (STATE_HANDLE.compareAndSet(e, NONE, PARKED)) {
                    return e;
                }
            }

            e.mState = NONE;
            return null;
        }

        private Entry entryFor(Thread thread) {
            int hash = hash(thread);

            // Quick check without synchronization.
            Entry e = findEntry(thread, hash);

            if (e == null) synchronized (this) {
                e = findEntry(thread, hash);
                if (e == null) {
                    if (mSize >= mEntries.length * 0.75 && !cleanup()) {
                        rehash();
                    }
                    e = new Entry(thread);
                    int ix = hash & (mEntries.length - 1);
                    e.mNext = mEntries[ix];
                    mEntries[ix] = e;
                    mSize++;
                }
            }

            return e;
        }

        private int hash(Thread thread) {
            int hash = Long.hashCode(thread.threadId());
            hash ^= (hash >>> 20) ^ (hash >>> 12);
            hash ^= (hash >>> 7) ^ (hash >>> 4);
            return hash;
        }

        private Entry findEntry(Thread thread, int hash) {
            Entry[] entries = mEntries;
            for (Entry e = entries[hash & (entries.length - 1)]; e != null; e = e.mNext) {
                if (e.refersTo(thread)) {
                    return e;
                }
            }
            return null;
        }

        private boolean cleanup() {
            int originalSize = mSize;

            Entry[] entries = mEntries;
            for (int i=0; i<entries.length; i++) {
                for (Entry e = entries[i], prev = null; e != null; ) {
                    Entry next = e.mNext;
                    Thread t = e.get();
                    if (t == null || t.getState() == Thread.State.TERMINATED) {
                        if (prev == null) {
                            entries[i] = next;
                        } else {
                            prev.mNext = next;
                        }
                        mSize--;
                    } else {
                        prev = e;
                    }
                    e = next;
                }
            }

            return mSize != originalSize;
        }

        private void rehash() {
            Entry[] entries = mEntries;
            var newEntries = new Entry[entries.length << 1];

            for (int i=0; i<entries.length; i++) {
                for (Entry e = entries[i]; e != null; ) {
                    Entry next = e.mNext;
                    Thread t = e.get();
                    if (t == null || t.getState() == Thread.State.TERMINATED) {
                        // Entries may have been already modified, so continue with rehash.
                        mSize--;
                    } else {
                        int ix = hash(t) & (newEntries.length - 1);
                        e.mNext = newEntries[ix];
                        newEntries[ix] = e;
                    }
                    e = next;
                }
            }

            mEntries = newEntries;
        }

        private static class Entry extends WeakReference<Thread> {
            Entry mNext;
            volatile int mState;

            Entry(Thread thread) {
                super(thread);
            }
        }
    }
}
