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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Shared pool of daemon threads. Intended as a faster alternative to launching new threads,
 * but not as fast as using a dedicated thread pool.
 *
 * @author Brian S O'Neill
 */
public final class Runner {
    private static final ThreadGroup cMainGroup;
    private static final ThreadPoolExecutor cMainExecutor;

    private static volatile ConcurrentHashMap<ThreadGroup, ThreadPoolExecutor> cExecutors;

    static {
        cMainGroup = obtainGroup();
        cMainExecutor = newExecutor(cMainGroup);
    }

    public static void start(Runnable command) {
        start(null, command);
    }

    /**
     * @param namePrefix name prefix to assign to the thread
     */
    public static void start(String namePrefix, Runnable command) {
        Runnable wrapped = () -> {
            final Thread t = Thread.currentThread();
            final String name = t.getName();

            if (namePrefix != null) {
                setName(t, namePrefix);
            }

            try {
                command.run();
            } catch (Throwable e) {
                t.getUncaughtExceptionHandler().uncaughtException(t, e);
            }

            t.setPriority(Thread.NORM_PRIORITY);

            if (!name.equals(t.getName())) {
                t.setName(name);
            }
        };

        ThreadGroup group = obtainGroup();

        ThreadPoolExecutor executor;
        if (group == cMainGroup) {
            executor = cMainExecutor;
        } else {
            ConcurrentHashMap<ThreadGroup, ThreadPoolExecutor> executors = cExecutors;

            if (executors == null) {
                synchronized (Runner.class) {
                    executors = cExecutors;
                    if (executors == null) {
                        cExecutors = new ConcurrentHashMap<>();
                    }
                }
            }

            executor = executors.get(group);

            if (executor == null) {
                synchronized (Runner.class) {
                    executor = executors.get(group);
                    if (executor == null) {
                        executor = newExecutor(group);
                        cExecutors.put(group, executor);
                    }
                }
            }
        }

        executor.execute(wrapped);
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

    private static ThreadPoolExecutor newExecutor(ThreadGroup group) {
        return new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                                      60L, TimeUnit.SECONDS,
                                      new SynchronousQueue<Runnable>(),
                                      new Factory(group));
    }

    private static void setName(Thread t, String namePrefix) {
        t.setName(namePrefix + '-' + Long.toUnsignedString(t.getId()));
    }

    private Runner() { }

    private static class Factory implements ThreadFactory {
        private final ThreadGroup mGroup;

        Factory(ThreadGroup group) {
            mGroup = group;
        }

        @Override
        public Thread newThread(Runnable r) {
            var t = new Thread(mGroup, r);
            setName(t, "Runner-" + mGroup.getName());
            t.setDaemon(true);
            return t;
        }
    }
}
