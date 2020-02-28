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

import java.util.List;
import java.util.Objects;

import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Shared pool of daemon threads. Intended as a faster alternative to launching new threads,
 * but not as fast as a work stealing pool.
 *
 * @author Brian S O'Neill
 */
public final class Runner extends AbstractExecutorService {
    private static final ThreadGroup cMainGroup;
    private static final Runner cMainRunner;

    private static volatile ConcurrentHashMap<ThreadGroup, Runner> cRunners;

    static {
        cMainGroup = obtainGroup();
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
     * Return an executor for the current thread's group or security manager.
     */
    public static Runner current() {
        ThreadGroup group = obtainGroup();

        if (group == cMainGroup) {
            return cMainRunner;
        }

        ConcurrentHashMap<ThreadGroup, Runner> runners = cRunners;

        if (runners == null) {
            synchronized (Runner.class) {
                runners = cRunners;
                if (runners == null) {
                    cRunners = new ConcurrentHashMap<>();
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

    private static ThreadGroup obtainGroup() {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            ThreadGroup group = sm.getThreadGroup();
            if (group != null) {
                return group;
            }
        }

        return Thread.currentThread().getThreadGroup();
    }

    private static void setThreadName(Thread t, String namePrefix) {
        t.setName(namePrefix + '-' + Long.toUnsignedString(t.getId()));
    }

    private final ThreadGroup mGroup;
    private final Latch mLatch;

    private Loop mReady;

    private Runner(ThreadGroup group) {
        mGroup = group;
        mLatch = new Latch();
    }

    @Override
    public void execute(Runnable task) {
        if (mLatch.tryAcquireExclusive()) {
            try {
                doExecute(task);
            } finally {
                mLatch.releaseExclusive();
            }
        } else {
            mLatch.uponExclusive(() -> doExecute(task));
        }
    }

    // Caller must hold exclusive latch.
    private void doExecute(Runnable task) {
        Loop ready = mReady;
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
            ready.mCondition.signal(mLatch);
        }
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public void shutdown() {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public List<Runnable> shutdownNow() {
        throw new UnsupportedOperationException();
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
     * @throws UnsupportedOperationException
     */
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    // Caller must hold exclusive latch.
    private void enqueue(Loop ready) {
        Loop prev = mReady;
        if (prev != null) {
            prev.mNext = ready;
            ready.mPrev = prev;
        }
        mReady = ready;
    }

    // Caller must hold exclusive latch, which is always released by this method.
    private void remove(Loop exiting) {
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
        mLatch.releaseExclusive();
    }

    private class Loop extends Thread {
        private static final long TIMEOUT = 50_000_000_000L;
        private static final long JITTER = 10_000_000_000L;

        private final LatchCondition mCondition;

        private Loop mPrev, mNext;

        private Runnable mTask;

        Loop(Runnable task) {
            super(mGroup, (Runnable) null);
            mCondition = new LatchCondition();
            setDaemon(true);
            setThreadName(this, "Runner-" + mGroup.getName());
            mTask = task;
        }

        @Override
        public void run() {
            final String name = getName();
            Runnable task = mTask;

            while (true) {
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

                long timeout = TIMEOUT + ThreadLocalRandom.current().nextLong(JITTER);

                Latch latch = mLatch;
                latch.acquireExclusive();
                enqueue(this);
                int result = mCondition.await(latch, timeout, System.nanoTime() + timeout);
                task = mTask;

                if (result <= 0 && task == null) {
                    remove(this);
                    return;
                }

                latch.releaseExclusive();
            }
        }
    }
}
