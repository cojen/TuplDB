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

package org.cojen.tupl;

import java.io.IOException;

import java.lang.ref.WeakReference;
import java.lang.ref.ReferenceQueue;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicInteger;

import org.cojen.tupl.util.Latch;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class Checkpointer implements Runnable {
    private static final int STATE_INIT = 0, STATE_RUNNING = 1, STATE_CLOSED = 2;

    private final AtomicInteger mSuspendCount;
    private final ReferenceQueue<AbstractDatabase> mRefQueue;
    private final WeakReference<AbstractDatabase> mDatabaseRef;
    private final long mRateNanos;
    private final long mSizeThreshold;
    private final long mDelayThresholdNanos;
    private volatile Thread mThread;
    private volatile int mState;
    private Thread mShutdownHook;
    private List<ShutdownHook> mToShutdown;

    // Is null when extra checkpoint threads aren't enabled.
    private final ThreadPoolExecutor mExtraExecutor;

    /**
     * @param extraLimit maximum number of extra checkpoint threads to use
     */
    Checkpointer(AbstractDatabase db, DatabaseConfig config, int extraLimit) {
        mSuspendCount = new AtomicInteger();

        mRateNanos = config.mCheckpointRateNanos;
        mSizeThreshold = config.mCheckpointSizeThreshold;
        mDelayThresholdNanos = config.mCheckpointDelayThresholdNanos;

        if (mRateNanos < 0) {
            mRefQueue = new ReferenceQueue<>();
            mDatabaseRef = new WeakReference<>(db, mRefQueue);
        } else {
            mRefQueue = null;
            mDatabaseRef = new WeakReference<>(db);
        }

        ThreadPoolExecutor extraExecutor;
        {
            int max = config.mMaxCheckpointThreads;
            if (max < 0) {
                max = (-max * Runtime.getRuntime().availableProcessors());
            }

            max = Math.min(max, extraLimit) - 1;

            if (max <= 0) {
                extraExecutor = null;
            } else {
                long timeoutNanos = Math.max
                    (config.mCheckpointRateNanos, config.mCheckpointDelayThresholdNanos);
                if (timeoutNanos < 0) {
                    // One minute default.
                    timeoutNanos = TimeUnit.MINUTES.toNanos(1);
                }
                // Add one more second, with wraparound check.
                timeoutNanos += TimeUnit.SECONDS.toNanos(1);
                if (timeoutNanos < 0) {
                    timeoutNanos = Long.MAX_VALUE;
                }

                extraExecutor = new ThreadPoolExecutor
                    (max, max, timeoutNanos, TimeUnit.NANOSECONDS,
                     new LinkedBlockingQueue<>(), Checkpointer::newThread);

                extraExecutor.allowCoreThreadTimeOut(true);
            }
        }

        mExtraExecutor = extraExecutor;
    }

    /**
     * @param initialCheckpoint true to perform an initial checkpoint in the new thread
     */
    void start(boolean initialCheckpoint) {
        if (!initialCheckpoint) {
            mState = STATE_RUNNING;
        }
        mThread = newThread(this);
        mThread.start();
    }

    private static Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setDaemon(true);
        t.setName("Checkpointer-" + Long.toUnsignedString(t.getId()));
        return t;
    }

    @Override
    public void run() {
        try {
            if (mState == STATE_INIT) {
                // Start with an initial forced checkpoint.
                AbstractDatabase db = mDatabaseRef.get();
                if (db != null) {
                    db.checkpoint();
                }
                mState = STATE_RUNNING;
            }

            if (mRefQueue != null) {
                // When the checkpoint rate is negative (infinite delay), this thread is
                // suspended until the database isn't referenced anymore, or until the database
                // is explicitly closed.
                mRefQueue.remove();
                close(null);
                return;
            }

            long lastDurationNanos = 0;

            while (true) {
                long delayMillis = (mRateNanos - lastDurationNanos) / 1000000L;
                if (delayMillis > 0) {
                    Thread.sleep(delayMillis); 
                }

                AbstractDatabase db = mDatabaseRef.get();
                if (db == null) {
                    close(null);
                    return;
                }

                if (mSuspendCount.get() != 0) {
                    // Don't actually suspend the thread, allowing for weak reference checks.
                    lastDurationNanos = 0;
                } else try {
                    long startNanos = System.nanoTime();
                    db.checkpoint(false, mSizeThreshold, mDelayThresholdNanos);
                    long endNanos = System.nanoTime();

                    lastDurationNanos = endNanos - startNanos;
                } catch (DatabaseException e) {
                    EventListener listener = db.eventListener();
                    if (listener != null) {
                        listener.notify(EventType.CHECKPOINT_FAILED, "Checkpoint failed: %1$s", e);
                    }
                    if (!e.isRecoverable()) {
                        throw e;
                    }
                    lastDurationNanos = 0;
                }
            }
        } catch (Throwable e) {
            if (mState != STATE_CLOSED) {
                AbstractDatabase db = mDatabaseRef.get();
                if (db != null) {
                    Utils.closeQuietly(db, e);
                }
            }
            close(e);
        }
    }

    /**
     * Register to close the given object on shutdown or when the Database is
     * no longer referenced. The Shutdown object must not maintain a strong
     * reference to the Database.
     *
     * @param obj ignored if null
     * @return false if immediately shutdown
     */
    boolean register(ShutdownHook obj) {
        if (obj == null) {
            return false;
        }

        doRegister: if (mState != STATE_CLOSED) {
            synchronized (this) {
                if (mState == STATE_CLOSED) {
                    break doRegister;
                }

                if (mShutdownHook == null) {
                    Thread hook = new Thread(() -> Checkpointer.this.close(null));
                    try {
                        Runtime.getRuntime().addShutdownHook(hook);
                        mShutdownHook = hook;
                    } catch (IllegalStateException e) {
                        break doRegister;
                    }
                }

                if (mToShutdown == null) {
                    mToShutdown = new ArrayList<>(2);
                }

                mToShutdown.add(obj);
                return true;
            }
        }

        obj.shutdown();
        return false;
    }

    void suspend() {
        suspend(+1);
    }

    void resume() {
        suspend(-1);
    }

    private void suspend(int amt) {
        while (true) {
            int count = mSuspendCount.get() + amt;
            if (count < 0) {
                // Overflowed or too many resumes.
                throw new IllegalStateException();
            }
            if (mSuspendCount.compareAndSet(count - amt, count)) {
                break;
            }
        }
    }

    /**
     * Expected to only be implemented by the NodeContext class.
     */
    static interface DirtySet {
        /**
         * Flush all nodes matching the given state. Only one flush at a time is allowed.
         *
         * @param dirtyState the old dirty state to match on; CACHED_DIRTY_0 or CACHED_DIRTY_1
         */
        void flushDirty(int dirtyState) throws IOException;
    }

    void flushDirty(DirtySet[] dirtySets, int dirtyState) throws IOException {
        if (mExtraExecutor == null) {
            for (DirtySet set : dirtySets) {
                set.flushDirty(dirtyState);
            }
            return;
        }

        final class Countdown extends Latch {
            volatile Throwable mException;

            Countdown(int count) {
                super(count);
            }

            void failed(Throwable ex) {
                if (mException == null) {
                    // Compare-and-set is probably overkill here.
                    mException = ex;
                }
                releaseShared();
            }
        }

        final Countdown cd = new Countdown(dirtySets.length);

        for (DirtySet set : dirtySets) {
            mExtraExecutor.execute(() -> {
                try {
                    set.flushDirty(dirtyState);
                } catch (Throwable e) {
                    cd.failed(e);
                    return;
                }
                cd.releaseShared();
            });
        }

        Runnable task;
        while ((task = mExtraExecutor.getQueue().poll()) != null) {
            task.run();
        }

        cd.acquireExclusive();

        Throwable ex = cd.mException;

        if (ex != null) {
            Utils.rethrow(ex);
        }
    }

    boolean isClosed() {
        return mState == STATE_CLOSED;
    }

    void close(Throwable cause) {
        mState = STATE_CLOSED;
        mDatabaseRef.enqueue();
        mDatabaseRef.clear();

        List<ShutdownHook> toShutdown;
        synchronized (this) {
            if (mShutdownHook != null) {
                try {
                    Runtime.getRuntime().removeShutdownHook(mShutdownHook);
                } catch (Throwable e) {
                }
                mShutdownHook = null;
            }

            // Only run shutdown hooks if cleanly closing, to avoid deadlocks.
            if (mToShutdown == null || cause != null) {
                toShutdown = null;
            } else {
                toShutdown = new ArrayList<>(mToShutdown);
            }

            mToShutdown = null;
        }

        if (toShutdown != null) {
            for (ShutdownHook obj : toShutdown) {
                obj.shutdown();
            }
        }
    }

    /**
     * Interrupt all running threads, after calling close. Returns a thread to join, unless
     * checkpointer was never started.
     */
    Thread interrupt() {
        if (mExtraExecutor != null) {
            mExtraExecutor.shutdownNow();
        }

        Thread t = mThread;
        if (t != null) {
            mThread = null;
            t.interrupt();
        }

        return t;
    }
}
