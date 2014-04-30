/*
 *  Copyright 2012-2013 Brian S O'Neill
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

import java.lang.ref.WeakReference;
import java.lang.ref.ReferenceQueue;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class Checkpointer implements Runnable {
    private static int cThreadCounter;

    private final AtomicInteger mSuspendCount;
    private final ReferenceQueue<Database> mRefQueue;
    private final WeakReference<Database> mDatabaseRef;
    private final long mRateNanos;
    private final long mSizeThreshold;
    private final long mDelayThresholdNanos;
    private volatile boolean mClosed;
    private Hook mShutdownHook;
    private List<Shutdown> mToShutdown;

    Checkpointer(Database db, DatabaseConfig config) {
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
    }

    void start() {
        int num;
        synchronized (Checkpointer.class) {
            num = ++cThreadCounter;
        }
        Thread t = new Thread(this);
        t.setDaemon(true);
        t.setName("Checkpointer-" + (num & 0xffffffffL));
        t.start();
    }

    @Override
    public void run() {
        try {
            if (mRefQueue != null) {
                mRefQueue.remove();
                close();
                return;
            }

            long lastDurationNanos = 0;

            while (true) {
                long delayMillis = (mRateNanos - lastDurationNanos) / 1000000L;
                if (delayMillis > 0) {
                    Thread.sleep(delayMillis); 
                }

                Database db = mDatabaseRef.get();
                if (db == null) {
                    close();
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
                    if (!e.isRecoverable()) {
                        throw e;
                    }
                    lastDurationNanos = 0;
                }
            }
        } catch (Throwable e) {
            if (!mClosed) {
                Database db = mDatabaseRef.get();
                if (db != null && !db.mClosed) {
                    Utils.closeQuietly(null, db, e);
                }
            }
            close();
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
    boolean register(Shutdown obj) {
        if (obj == null) {
            return false;
        }

        doRegister: if (!mClosed) {
            synchronized (this) {
                if (mClosed) {
                    break doRegister;
                }

                if (mShutdownHook == null) {
                    Hook hook = new Hook(this);
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

    void close() {
        mClosed = true;
        mDatabaseRef.enqueue();
        mDatabaseRef.clear();

        List<Shutdown> toShutdown;
        synchronized (this) {
            if (mShutdownHook != null) {
                try {
                    Runtime.getRuntime().removeShutdownHook(mShutdownHook);
                } catch (Throwable e) {
                }
                mShutdownHook = null;
            }

            if (mToShutdown == null) {
                toShutdown = null;
            } else {
                toShutdown = new ArrayList<>(mToShutdown);
                mToShutdown = null;
            }
        }

        if (toShutdown != null) {
            for (Shutdown obj : toShutdown) {
                obj.shutdown();
            }
        }
    }

    public static interface Shutdown {
        void shutdown();
    }

    static class Hook extends Thread {
        private final Checkpointer mCheckpointer;

        Hook(Checkpointer c) {
            mCheckpointer = c;
        }

        @Override
        public void run() {
            mCheckpointer.close();
        }
    }
}
