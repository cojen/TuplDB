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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.locks.LockSupport;

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

    static final VarHandle cSizeHandle, cFirstHandle, cLastHandle, cStateHandle, cThreadHandle;

    static {
        try {
            // Reduce the risk of "lost unpark" due to classloading.
            // https://bugs.openjdk.java.net/browse/JDK-8074773
            Class<?> clazz = LockSupport.class;

            cSizeHandle =
                MethodHandles.lookup().findVarHandle
                (Worker.class, "mSize", int.class);

            cFirstHandle =
                MethodHandles.lookup().findVarHandle
                (Worker.class, "mFirst", Task.class);

            cLastHandle =
                MethodHandles.lookup().findVarHandle
                (Worker.class, "mLast", Task.class);

            cStateHandle =
                MethodHandles.lookup().findVarHandle
                (Worker.class, "mThreadState", int.class);

            cThreadHandle =
                MethodHandles.lookup().findVarHandle
                (Worker.class, "mThread", Thread.class);
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

        if (!cSizeHandle.compareAndSet(this, size, size + 1)) {
            cSizeHandle.getAndAdd(this, 1);
        }

        Task prev = (Task) cLastHandle.getAndSet(this, task);
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
                    cSizeHandle.getAndAdd(this, -1);
                    mThreadState = THREAD_NONE;
                    throw e;
                }
                mThread = t;
                return true;
            }

            // assert state == THREAD_IDLE

            if (cStateHandle.compareAndSet(this, state, THREAD_RUNNING)) {
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
            if (cStateHandle.compareAndSet(this, THREAD_RUNNING, THREAD_BLOCKED)) {
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
            if (cStateHandle.compareAndSet(this, THREAD_RUNNING, THREAD_BLOCKED)) {
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
    public static abstract class Task {
        volatile Task mNext;

        public abstract void run() throws Throwable;
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
                            cLastHandle.compareAndSet(this, task, null))
                        {
                            cFirstHandle.compareAndSet(this, task, null);
                            break;
                        }
                    }
                }

                try {
                    task.run();
                } catch (Throwable e) {
                    Utils.uncaught(e);
                }

                size = ((int) cSizeHandle.getAndAdd(this, -1)) - 1;

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

            if (!cStateHandle.compareAndSet(this, THREAD_RUNNING, THREAD_IDLE)) {
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
                    if (!cStateHandle.compareAndSet(this, THREAD_IDLE, THREAD_NONE)) {
                        continue outer;
                    }
                    cThreadHandle.compareAndSet(this, Thread.currentThread(), null);
                    return;
                }

                if (mThreadState != THREAD_IDLE) {
                    continue outer;
                }
            }

            cStateHandle.compareAndSet(this, THREAD_IDLE, THREAD_RUNNING);
        }
    }
}
