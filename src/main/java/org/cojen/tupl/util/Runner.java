/*
 *  Copyright 2020 Cojen.org
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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Implements an unbounded pool of daemon threads which automatically exit when idle. Intended
 * as a faster alternative to launching new threads, but not as fast as a work stealing pool.
 *
 * @author Brian S O'Neill
 */
public final class Runner extends AbstractExecutorService {
    private static final ThreadGroup cMainGroup;
    private static final Runner cMainRunner;

    private static volatile ConcurrentHashMap<ThreadGroup, Runner> cRunners;

    static {
        cMainGroup = Thread.currentThread().getThreadGroup();
        cMainRunner = new Runner(cMainGroup);
    }

    public static void start(Runnable command) {
        start(null, command);
    }

    /**
     * @param namePrefix name prefix to assign to the thread
     */
    public static void start(final String namePrefix, final Runnable command) {
        Objects.requireNonNull(command);

        Runnable actual = command;

        if (namePrefix != null) {
            actual = () -> {
                setThreadName(Thread.currentThread(), namePrefix);
                command.run();
            };
        }

        current().execute(actual);
    }

    /**
     * Return an executor for the current thread's group.
     */
    public static Runner current() {
        ThreadGroup group = Thread.currentThread().getThreadGroup();

        if (group == cMainGroup) {
            return cMainRunner;
        }

        ConcurrentHashMap<ThreadGroup, Runner> runners = cRunners;

        if (runners == null) {
            synchronized (Runner.class) {
                runners = cRunners;
                if (runners == null) {
                    cRunners = runners = new ConcurrentHashMap<>();
                }
            }
        }

        Runner runner = runners.get(group);

        if (runner == null) {
            synchronized (Runner.class) {
                runner = runners.get(group);
                if (runner == null) {
                    runner = new Runner(group);
                    cRunners.put(group, runner);
                }
            }
        }

        return runner;
    }

    private static void setThreadName(Thread t, String namePrefix) {
        t.setName(namePrefix + '-' + Long.toUnsignedString(t.threadId()));
    }

    private final ThreadGroup mGroup;

    private Loop mReady;

    private Runner(ThreadGroup group) {
        mGroup = group;
    }

    @Override
    public void execute(Runnable task) {
        Loop ready;
        synchronized (this) {
            ready = mReady;
            if (ready == null) {
                new Loop(task).start();
            } else {
                Loop prev = ready.mPrev;
                if (prev != null) {
                    prev.mNext = null;
                    ready.mPrev = null;
                }
                mReady = prev;
                ready.mTask = task;
            }
        }
        Parker.unpark(ready);
    }

    /**
     * Does nothing.
     */
    @Override
    public void shutdown() {
    }

    /**
     * Does nothing.
     *
     * @return empty list
     */
    @Override
    public List<Runnable> shutdownNow() {
        return Collections.emptyList();
    }

    /**
     * @return false
     */
    @Override
    public boolean isShutdown() {
        return false;
    }

    /**
     * @return false
     */
    @Override
    public boolean isTerminated() {
        return false;
    }

    /**
     * @return false
     */
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    private synchronized void enqueue(Loop ready) {
        Loop prev = mReady;
        if (prev != null) {
            prev.mNext = ready;
            ready.mPrev = prev;
        }
        mReady = ready;
    }

    /**
     * @return null if removed, else a task to run
     */
    private synchronized Runnable tryRemove(Loop exiting) {
        Runnable task = exiting.mTask;
        if (task != null) {
            return task;
        }
        Loop prev = exiting.mPrev;
        Loop next = exiting.mNext;
        if (next != null) {
            next.mPrev = prev;
        } else {
            mReady = prev;
        }
        if (prev != null) {
            prev.mNext = next;
        }
        return null;
    }

    private class Loop extends Thread {
        private static final long TIMEOUT = 50_000_000_000L;
        private static final long JITTER = 10_000_000_000L;

        private Loop mPrev, mNext;

        private volatile boolean mStarted;

        private volatile Runnable mTask;

        Loop(Runnable task) {
            super(mGroup, (Runnable) null);
            setDaemon(true);
            setThreadName(this, "Runner-" + mGroup.getName());
            mTask = task;
        }

        @Override
        public void run() {
            if (mStarted) {
                return;
            }

            mStarted = true;

            final String name = getName();
            Runnable task = mTask;

            outer: while (true) {
                mTask = null;

                if (isInterrupted()) {
                    // Clear interrupt status.
                    Thread.interrupted();
                }

                try {
                    task.run();
                } catch (Throwable e) {
                    try {
                        getUncaughtExceptionHandler().uncaughtException(this, e);
                    } catch (Throwable e2) {
                        // Ignore.
                    }
                }

                if (getPriority() != NORM_PRIORITY) {
                    setPriority(NORM_PRIORITY);
                }

                if (!name.equals(getName())) {
                    setName(name);
                }

                task = null;

                enqueue(this);

                long timeout = TIMEOUT + ThreadLocalRandom.current().nextLong(JITTER);
                long end = System.nanoTime() + timeout;

                do {
                    Parker.parkNanos(this, timeout);
                    task = mTask;
                    if (task != null) {
                        continue outer;
                    }
                    timeout = end - System.nanoTime();
                } while (timeout > 0);

                if ((task = tryRemove(this)) == null) {
                    return;
                }
            }
        }
    }
}
