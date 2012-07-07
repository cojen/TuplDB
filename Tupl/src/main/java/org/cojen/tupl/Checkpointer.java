/*
 *  Copyright 2012 Brian S O'Neill
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

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class Checkpointer implements Runnable {
    private static int cThreadCounter;

    private final ReferenceQueue<Database> mRefQueue;
    private final WeakReference<Database> mDatabaseRef;
    private final long mRateNanos;
    private volatile boolean mClosed;
    private Hook mShutdownHook;
    private List<Shutdown> mToShutdown;

    Checkpointer(Database db, long rateNanos) {
        if (rateNanos < 0) {
            mRefQueue = new ReferenceQueue<Database>();
            mDatabaseRef = new WeakReference<Database>(db, mRefQueue);
        } else {
            mRefQueue = null;
            mDatabaseRef = new WeakReference<Database>(db);
        }

        mRateNanos = rateNanos;
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

                //System.out.println("checkpoint...");
                long startNanos = System.nanoTime();
                db.checkpoint();
                long endNanos = System.nanoTime();
                //System.out.println("...done");

                lastDurationNanos = endNanos - startNanos;
            }
        } catch (Exception e) {
            if (!mClosed) {
                Utils.uncaught(e);
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
                    mToShutdown = new ArrayList<Shutdown>(2);
                }

                mToShutdown.add(obj);
                return true;
            }
        }

        obj.shutdown();
        return false;
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
                toShutdown = new ArrayList<Shutdown>(mToShutdown);
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
