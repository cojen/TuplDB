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

package org.cojen.tupl.core;

import java.io.IOException;

import java.lang.ref.WeakReference;
import java.lang.ref.ReferenceQueue;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.cojen.tupl.DatabaseException;

import org.cojen.tupl.diag.EventListener;
import org.cojen.tupl.diag.EventType;

import org.cojen.tupl.util.Latch;

/**
 * Runs the background task to checkpoint databases, and also tracks all the database shutdown
 * hooks. This class also launches parallel checkpoint flush tasks, when using the {@link
 * DatabaseConfig#maxCheckpointThreads} option.
 *
 * @author Brian S O'Neill
 */
final class Checkpointer extends Latch implements Runnable {
    private final ReferenceQueue<LocalDatabase> mRefQueue;
    private final WeakReference<LocalDatabase> mDatabaseRef;
    private final long mRateNanos;
    private final long mSizeThreshold;
    private final long mDelayThresholdNanos;
    private volatile Thread mThread;
    private volatile boolean mClosed;
    private Thread mShutdownHook;
    private List<ShutdownHook> mToShutdown;

    // Is null when extra checkpoint threads aren't enabled.
    private final ThreadPoolExecutor mExtraExecutor;

    private volatile int mSuspendCount;

    /**
     * @param extraLimit maximum number of extra checkpoint threads to use
     */
    Checkpointer(LocalDatabase db, Launcher launcher, int extraLimit) {
        mRateNanos = launcher.mCheckpointRateNanos;
        mSizeThreshold = launcher.mCheckpointSizeThreshold;
        mDelayThresholdNanos = launcher.mCheckpointDelayThresholdNanos;

        if (mRateNanos < 0) {
            mRefQueue = new ReferenceQueue<>();
            mDatabaseRef = new WeakReference<>(db, mRefQueue);
        } else {
            mRefQueue = null;
            mDatabaseRef = new WeakReference<>(db);
        }

        ThreadPoolExecutor extraExecutor;
        {
            int max = launcher.mMaxCheckpointThreads;
            if (max < 0) {
                max = (-max * Runtime.getRuntime().availableProcessors());
            }

            max = Math.min(max, extraLimit) - 1;

            if (max <= 0) {
                extraExecutor = null;
            } else {
                // Default keep-alive time for extra checkpoint threads.
                long keepAliveSeconds = 60;

                // If automatic checkpoint rate is close to the keep-alive time, boost the
                // keep-alive to prevent premature thread exits.
                long rateSeconds = TimeUnit.NANOSECONDS.toSeconds(launcher.mCheckpointRateNanos);
                if (rateSeconds <= keepAliveSeconds * 2) {
                    keepAliveSeconds = Math.max(keepAliveSeconds, rateSeconds + 5);
                }

                extraExecutor = new ThreadPoolExecutor
                    (max, max, keepAliveSeconds, TimeUnit.SECONDS,
                     new LinkedBlockingQueue<>(), Checkpointer::newThread);

                extraExecutor.allowCoreThreadTimeOut(true);
            }
        }

        mExtraExecutor = extraExecutor;
    }

    void start() {
        Thread t = newThread(this);
        t.start();
        mThread = t;
    }

    boolean isStarted() {
        return mThread != null;
    }

    private static Thread newThread(Runnable r) {
        var t = new Thread(r);
        t.setDaemon(true);
        t.setName("Checkpointer-" + Long.toUnsignedString(t.threadId()));
        return t;
    }

    @Override
    public void run() {
        try {
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

                LocalDatabase db = mDatabaseRef.get();
                if (db == null) {
                    close(null);
                    return;
                }

                if (isSuspended()) {
                    // Don't actually suspend the thread, allowing for weak reference checks.
                    lastDurationNanos = 0;
                } else try {
                    long startNanos = System.nanoTime();
                    db.checkpoint(mSizeThreshold, mDelayThresholdNanos);
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
            if (!mClosed) {
                LocalDatabase db = mDatabaseRef.get();
                if (db != null) {
                    Utils.closeQuietly(db, e);
                }
            }
            close(e);
        }
    }

    /**
     * Register to close the given object on shutdown or when the LocalDatabase is no longer
     * referenced. The Shutdown object must not maintain a strong reference to the
     * LocalDatabase.
     *
     * @param obj ignored if null
     * @return false if immediately shutdown
     */
    boolean register(ShutdownHook obj) {
        if (obj == null) {
            return false;
        }

        doRegister: if (!mClosed) {
            synchronized (this) {
                if (mClosed) {
                    break doRegister;
                }

                if (mShutdownHook == null) {
                    var hook = new Thread(() -> Checkpointer.this.close(null));
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

    boolean isSuspended() {
        return mSuspendCount != 0;
    }

    private void suspend(int amt) {
        acquireExclusive();
        try {
            int count = mSuspendCount + amt;
            if (count < 0) {
                // Overflowed or too many resumes.
                throw new IllegalStateException();
            }
            mSuspendCount = count;
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Returns true if automatic checkpoints are currently enabled.
     */
    boolean isEnabled() {
        return mRateNanos >= 0 && isStarted() && !isSuspended();
    }

    /**
     * Expected to only be implemented by the NodeGroup class.
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

        final var countdown = new Latch(dirtySets.length) {
            volatile Throwable mException;

            void failed(Throwable ex) {
                if (mException == null) {
                    // Compare-and-set is probably overkill here.
                    mException = ex;
                }
                releaseShared();
            }
        };

        for (DirtySet set : dirtySets) {
            mExtraExecutor.execute(() -> {
                try {
                    set.flushDirty(dirtyState);
                } catch (Throwable e) {
                    countdown.failed(e);
                    return;
                }
                countdown.releaseShared();
            });
        }

        Runnable task;
        while ((task = mExtraExecutor.getQueue().poll()) != null) {
            task.run();
        }

        countdown.acquireExclusive();

        Throwable ex = countdown.mException;

        if (ex != null) {
            Utils.rethrow(ex);
        }
    }

    void close(Throwable cause) {
        mClosed = true;
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
        Thread t = mThread;
        if (t != null) {
            mThread = null;
            t.interrupt();
        }

        return t;
    }

    /**
     * Shutdown any thread pool, after calling close, interrupt, and running any final checkpoint.
     */
    void shutdown() {
        if (mExtraExecutor != null) {
            mExtraExecutor.shutdownNow();
        }
    }
}
