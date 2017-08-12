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

package org.cojen.tupl.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.locks.LockSupport;

import org.cojen.tupl.io.UnsafeAccess;
import org.cojen.tupl.io.Utils;

/**
 * Simple task worker which has at most one background thread, and is expected to have only one
 * thread enqueing tasks. This class isn't thread safe for enqueuing tasks, and so the caller
 * must provide its own mutual exclusion to protect against concurrent enqueues.
 *
 * @author Brian S O'Neill
 * @see WorkerGroup
 */
@SuppressWarnings("restriction")
public class Worker {
    /**
     * @param maxSize maximum amount of tasks which can be enqueued
     * @param keepAliveTime maximum idle time before worker thread exits
     * @param unit keepAliveTime time unit
     * @param threadFactory null for default
     */
    public static Worker make(int maxSize, long keepAliveTime, TimeUnit unit,
                              ThreadFactory threadFactory)
    {
        if (maxSize <= 0) {
            throw new IllegalArgumentException();
        }

        if (threadFactory == null) {
            threadFactory = Executors.defaultThreadFactory();
        }

        return new Worker(maxSize, keepAliveTime, unit, threadFactory);
    }

    private static final sun.misc.Unsafe UNSAFE = UnsafeAccess.obtain();

    static final long SIZE_OFFSET, FIRST_OFFSET, LAST_OFFSET, STATE_OFFSET, THREAD_OFFSET;

    static {
        try {
            // Reduce the risk of "lost unpark" due to classloading.
            // https://bugs.openjdk.java.net/browse/JDK-8074773
            Class<?> clazz = LockSupport.class;

            SIZE_OFFSET = UNSAFE.objectFieldOffset(Worker.class.getDeclaredField("mSize"));
            FIRST_OFFSET = UNSAFE.objectFieldOffset(Worker.class.getDeclaredField("mFirst"));
            LAST_OFFSET = UNSAFE.objectFieldOffset(Worker.class.getDeclaredField("mLast"));
            STATE_OFFSET = UNSAFE.objectFieldOffset(Worker.class.getDeclaredField("mThreadState"));
            THREAD_OFFSET = UNSAFE.objectFieldOffset(Worker.class.getDeclaredField("mThread"));
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    private final ThreadFactory mThreadFactory;
    private final int mMaxSize;
    private final long mKeepAliveNanos;

    private volatile int mSize;
    private volatile Task mFirst;
    private volatile Task mLast;

    private static final int
        THREAD_NONE = 0,    // no worker thread
        THREAD_RUNNING = 1, // worker thread is running
        THREAD_BLOCKED = 2, // worker thread is running and an enqueue/join thread is blocked
        THREAD_IDLE = 3;    // worker thread is idle

    private volatile int mThreadState;
    private volatile Thread mThread;

    private Thread mWaiter;

    private Worker(int maxSize, long keepAliveTime, TimeUnit unit, ThreadFactory threadFactory) {
        mThreadFactory = threadFactory;

        mMaxSize = maxSize;

        if (keepAliveTime > 0) {
            mKeepAliveNanos = unit.toNanos(keepAliveTime);
        } else {
            mKeepAliveNanos = keepAliveTime;
        }
    }

    /**
     * Attempt to enqueue a task without blocking. When the task object is enqueued, it must
     * not be used again for any other tasks.
     *
     * @return false if queue is full and task wasn't enqueued
     */
    public boolean tryEnqueue(Task task) {
        if (task == null) {
            throw new NullPointerException();
        }

        int size = mSize;
        if (size >= mMaxSize) {
            return false;
        }

        if (!UNSAFE.compareAndSwapInt(this, SIZE_OFFSET, size, size + 1)) {
            UNSAFE.getAndAddInt(this, SIZE_OFFSET, 1);
        }

        Task prev = (Task) UNSAFE.getAndSetObject(this, LAST_OFFSET, task);
        if (prev == null) {
            mFirst = task;
        } else {
            prev.mNext = task;
        }

        while (true) {
            int state = mThreadState;

            if (state == THREAD_RUNNING) {
                return true;
            }

            if (state == THREAD_NONE) {
                mThreadState = THREAD_RUNNING;
                Thread t;
                try {
                    t = mThreadFactory.newThread(this::runTasks);
                    t.start();
                } catch (Throwable e) {
                    UNSAFE.getAndAddInt(this, SIZE_OFFSET, -1);
                    mThreadState = THREAD_NONE;
                    throw e;
                }
                mThread = t;
                return true;
            }

            // assert state == THREAD_IDLE

            if (UNSAFE.compareAndSwapInt(this, STATE_OFFSET, state, THREAD_RUNNING)) {
                LockSupport.unpark(mThread);
                return true;
            }
        }
    }

    /**
     * Enqueue a task, blocking if necessary until space is available. When the task object is
     * enqueued, it must not be used again for any other tasks.
     */
    public void enqueue(Task task) {
        while (!tryEnqueue(task)) {
            // Keep trying before parking.
            for (int i=1; i<Latch.SPIN_LIMIT; i++) {
                if (tryEnqueue(task)) {
                    return;
                }
            }
            Thread.yield();
            if (tryEnqueue(task)) {
                return;
            }
            mWaiter = Thread.currentThread();
            if (UNSAFE.compareAndSwapInt(this, STATE_OFFSET, THREAD_RUNNING, THREAD_BLOCKED)) {
                LockSupport.park(this);
            }
            mWaiter = null;
        }
    }

    /**
     * Waits until the worker queue is drained and possibly interrupt it. If the worker thread
     * is interrupted and exits, a new thread is started when new tasks are enqueued.
     *
     * @param interrupt pass true to interrupt the worker thread so that it exits
     */
    public void join(boolean interrupt) {
        while (mSize > 0) {
            // Keep trying before parking.
            for (int i=1; i<Latch.SPIN_LIMIT; i++) {
                if (mSize <= 0) {
                    break;
                }
            }
            Thread.yield();
            if (mSize <= 0) {
                break;
            }
            mWaiter = Thread.currentThread();
            if (UNSAFE.compareAndSwapInt(this, STATE_OFFSET, THREAD_RUNNING, THREAD_BLOCKED)) {
                LockSupport.park(this);
            }
            mWaiter = null;
        }

        if (interrupt) {
            Thread t = mThread;
            if (t != null) {
                t.interrupt();
            }
        }
    }

    /**
     * One-shot {@link Worker worker} task instance.
     */
    public static abstract class Task implements Runnable {
        volatile Task mNext;
    }

    private void runTasks() {
        int size = 0;

        outer: while (true) {
            if (size > 0 || (size = mSize) > 0) {
                Task task;
                while ((task = mFirst) == null);

                Task next;
                while (true) {
                    next = task.mNext;
                    if (next != null) {
                        mFirst = next;
                        break;
                    } else {
                        // Queue is now empty, unless an enqueue is in progress.
                        if (task == mLast &&
                            UNSAFE.compareAndSwapObject(this, LAST_OFFSET, task, null))
                        {
                            UNSAFE.compareAndSwapObject(this, FIRST_OFFSET, task, null);
                            break;
                        }
                    }
                }

                try {
                    task.run();
                } catch (Throwable e) {
                    Utils.uncaught(e);
                }

                size = UNSAFE.getAndAddInt(this, SIZE_OFFSET, -1) - 1;

                if (mThreadState == THREAD_BLOCKED) {
                    mThreadState = THREAD_RUNNING;
                    LockSupport.unpark(mWaiter);
                }

                continue;
            }

            // Keep trying before parking.

            // Start at zero to ensure at least one check for THREAD_BLOCKED state is made.
            for (int i=0; i<Latch.SPIN_LIMIT; i++) {
                if ((size = mSize) > 0) {
                    continue outer;
                }
                if (mThreadState == THREAD_BLOCKED) {
                    mThreadState = THREAD_RUNNING;
                    LockSupport.unpark(mWaiter);
                }
            }

            Thread.yield();

            if ((size = mSize) > 0) {
                continue;
            }

            if (!UNSAFE.compareAndSwapInt(this, STATE_OFFSET, THREAD_RUNNING, THREAD_IDLE)) {
                continue;
            }

            long parkNanos = mKeepAliveNanos;
            long endNanos = parkNanos < 0 ? 0 : (System.nanoTime() + parkNanos);

            while ((size = mSize) <= 0) {
                if (parkNanos < 0) {
                    LockSupport.park(this);
                } else {
                    LockSupport.parkNanos(this, parkNanos);
                    parkNanos = Math.max(0, endNanos - System.nanoTime());
                }

                boolean interrupted = Thread.interrupted();

                if ((size = mSize) > 0) {
                    break;
                }

                if (parkNanos == 0 || interrupted) {
                    if (!UNSAFE.compareAndSwapInt(this, STATE_OFFSET, THREAD_IDLE, THREAD_NONE)) {
                        continue outer;
                    }
                    UNSAFE.compareAndSwapObject(this, THREAD_OFFSET, Thread.currentThread(), null);
                    return;
                }

                if (mThreadState != THREAD_IDLE) {
                    continue outer;
                }
            }

            UNSAFE.compareAndSwapInt(this, STATE_OFFSET, THREAD_IDLE, THREAD_RUNNING);
        }
    }
}
