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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * A group of {@link Worker workers} for running tasks. This class isn't thread safe for
 * enqueuing tasks, and so the caller must provide its own mutual exclusion to protect against
 * concurrent enqueues.
 *
 * @author Brian S O'Neill
 */
public abstract class WorkerGroup {
    /**
     * @param workerCount number of workers
     * @param maxSize maximum amount of tasks which can be enqueued per worker
     * @param keepAliveTime maximum idle time before worker threads exit
     * @param unit keepAliveTime time unit per worker
     * @param threadFactory null for default
     */
    public static WorkerGroup make(int workerCount, int maxSize,
                                   long keepAliveTime, TimeUnit unit,
                                   ThreadFactory threadFactory)
    {
        if (workerCount < 1) {
            throw new IllegalArgumentException();
        }

        if (threadFactory == null) {
            threadFactory = Executors.defaultThreadFactory();
        }

        if (workerCount == 1) {
            return new One(Worker.make(maxSize, keepAliveTime, unit, threadFactory));
        }

        return new Many(workerCount, maxSize, keepAliveTime, unit, threadFactory);
    }

    /**
     * Attempts to select an available worker and enqueues a task without blocking. When the
     * task object is enqueued, it must not be used again for any other tasks.
     *
     * @return selected worker or null if all worker queues are full and task wasn't enqueued
     */
    public abstract Worker tryEnqueue(Worker.Task task);

    /**
     * Enqueue a task, blocking if necessary until space is available. When the task object is
     * enqueued, it must not be used again for any other tasks.
     *
     * @return selected worker
     */
    public abstract Worker enqueue(Worker.Task task);

    /**
     * Waits until all the worker queues are drained. If the worker threads are interrupted and
     * exit, new threads are started when new tasks are enqueued.
     *
     * @param interrupt pass true to interrupt the worker threads so that they exit
     */
    public abstract void join(boolean interrupt);

    private static final class One extends WorkerGroup {
        private final Worker mWorker;

        One(Worker worker) {
            mWorker = worker;
        }

        @Override
        public Worker tryEnqueue(Worker.Task task) {
            mWorker.tryEnqueue(task);
            return mWorker;
        }

        @Override
        public Worker enqueue(Worker.Task task) {
            mWorker.enqueue(task);
            return mWorker;
        }

        @Override
        public void join(boolean interrupt) {
            mWorker.join(interrupt);
        }
    }

    private static final class Many extends WorkerGroup {
        private final Worker[] mWorkers;
        private int mLastSelected;

        Many(int workerCount, int maxSize,
             long keepAliveTime, TimeUnit unit,
             ThreadFactory threadFactory)
        {
            Worker[] workers = new Worker[workerCount];

            for (int i=0; i<workers.length; i++) {
                workers[i] = Worker.make(maxSize, keepAliveTime, unit, threadFactory);
            }

            mWorkers = workers;
        }

        @Override
        public Worker tryEnqueue(Worker.Task task) {
            // Start the search just lower than the last one selected, to drive tasks towards the
            // lower workers. The higher workers can then idle and allow their threads to exit.
            int slot = Math.max(0, mLastSelected - 1);

            for (int i=0; i<mWorkers.length; i++) {
                Worker w = mWorkers[slot];
                if (w.tryEnqueue(task)) {
                    mLastSelected = slot;
                    return w;
                }
                slot++;
                if (slot >= mWorkers.length) {
                    slot = 0;
                }
            }

            return null;
        }

        @Override
        public Worker enqueue(Worker.Task task) {
            Worker w = tryEnqueue(task);
            if (w == null) {
                w = mWorkers[mLastSelected = ThreadLocalRandom.current().nextInt(mWorkers.length)];
                w.enqueue(task);
            }
            return w;
        }

        @Override
        public void join(boolean interrupt) {
            for (Worker w : mWorkers) {
                w.join(interrupt);
            }
        }
    }
}
