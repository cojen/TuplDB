/*
 *  Copyright 2017 Cojen.org
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
public class Worker {
    private static final sun.misc.Unsafe UNSAFE = UnsafeAccess.obtain();

    static final long SIZE_OFFSET, FIRST_OFFSET, LAST_OFFSET, STATE_OFFSET, THREAD_OFFSET;

    static {
        try {
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
    private final int mNotifyAvailable;
    private final long mKeepAliveNanos;

    private volatile int mSize;
    private volatile Task mFirst;
    private volatile Task mLast;

    private static final int THREAD_NONE = 0, THREAD_RUNNING = 1, THREAD_WAITING = 2;
    private volatile int mThreadState;
    private volatile Thread mThread;

    private volatile Thread mWaiter;

    /**
     * @param maxSize maximum amount of tasks which can be enqueued
     * @param notifyAvailable minimum available space in the queue before worker can wake up a
     * blocked eneueue (pass zero for immediate wake up)
     * @param keepAliveTime maximum idle time before worker thread exits
     * @param unit keepAliveTime time unit
     * @param threadFactory null for default
     */
    public Worker(int maxSize, int notifyAvailable,
                  long keepAliveTime, TimeUnit unit,
                  ThreadFactory threadFactory)
    {
        if (maxSize <= 0 || notifyAvailable > maxSize) {
            throw new IllegalArgumentException();
        }

        if (threadFactory == null) {
            threadFactory = Executors.defaultThreadFactory();
        }

        mThreadFactory = threadFactory;

        mMaxSize = maxSize;
        mNotifyAvailable = notifyAvailable;

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

            // assert state == THREAD_WAITING
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
            LockSupport.park(this);
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
                    return;
                }
            }
            Thread.yield();
            if (mSize <= 0) {
                return;
            }
            mWaiter = Thread.currentThread();
            LockSupport.park(this);
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

                if ((mMaxSize - size) >= mNotifyAvailable) {
                    Thread waiter = mWaiter;
                    if (waiter != null) {
                        mWaiter = null;
                        LockSupport.unpark(waiter);
                    }
                }

                continue;
            }

            // Keep trying before parking.

            for (int i=1; i<Latch.SPIN_LIMIT; i++) {
                size = mSize;
                if (size > 0) {
                    continue outer;
                }
            }

            Thread.yield();

            size = mSize;
            if (size > 0) {
                continue;
            }

            mThreadState = THREAD_WAITING;

            size = mSize;
            if (size > 0) {
                mThreadState = THREAD_RUNNING;
                continue;
            }

            if (mKeepAliveNanos < 0) {
                LockSupport.park(this);
            } else {
                LockSupport.parkNanos(this, mKeepAliveNanos);
            }

            // Clear any interrupted state.
            Thread.interrupted();

            size = mSize;

            if (size > 0) {
                mThreadState = THREAD_RUNNING;
            } else if (mThreadState == THREAD_WAITING &&
                       UNSAFE.compareAndSwapInt(this, STATE_OFFSET, THREAD_WAITING, THREAD_NONE))
            {
                UNSAFE.compareAndSwapObject(this, THREAD_OFFSET, Thread.currentThread(), null);
                return;
            }
        }
    }
}
