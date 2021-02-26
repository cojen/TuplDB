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

package org.cojen.tupl.jmx;

import java.io.IOException;

import java.lang.management.ManagementFactory;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

import javax.management.JMException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.cojen.tupl.Database;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.util.Runner;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class Registration {
    private static final ReferenceQueue<Object> queue = new ReferenceQueue<>();

    public static void register(Database db, String base) {
        cleanup();
        base = sanitize(base);
        var server = ManagementFactory.getPlatformMBeanServer();
        try {
            var bean = new StandardMBean(new DbBean(db, base), DatabaseMBean.class);
            var name = newObjectName(base);
            if (server.isRegistered(name)) {
                server.unregisterMBean(name);
            }
            server.registerMBean(bean, name);
        } catch (JMException e) {
            throw Utils.rethrow(e);
        }
    }

    public static void unregister(String base) {
        cleanup();
        doUnregister(sanitize(base));
    }

    private static void doUnregister(String base) {
        var server = ManagementFactory.getPlatformMBeanServer();
        try {
            server.unregisterMBean(newObjectName(base));
        } catch (JMException e) {
            // Ignore.
        }
    }

    private static void cleanup() {
        Reference ref;
        while ((ref = queue.poll()) != null) {
            if (ref instanceof DbBean) {
                doUnregister(((DbBean) ref).mBase);
            }
        }
    }

    private static String sanitize(String base) {
        return base.replace(',', '_').replace('=', '_').replace(':', '_').replace('"', '_');
    }

    private static ObjectName newObjectName(String base) throws JMException {
        return new ObjectName("org.cojen.tupl", "database", base);
    }

    private static class DbBean extends WeakReference<Database> implements DatabaseMBean {
        private final String mBase;

        private Database.Stats mStats;
        private long mStatsTimestamp;

        private boolean mAsyncRunning;

        DbBean(Database db, String base) {
            super(db, queue);
            mBase = base;
        }

        @Override
        public long getFreeBytes() {
            var stats = stats();
            return stats == null ? 0 : stats.freePages * stats.pageSize;
        }

        @Override
        public long getTotalBytes() {
            var stats = stats();
            return stats == null ? 0 : stats.totalPages * stats.pageSize;
        }

        @Override
        public long getCacheBytes() {
            var stats = stats();
            return stats == null ? 0 : stats.cachePages * stats.pageSize;
        }

        @Override
        public long getDirtyBytes() {
            var stats = stats();
            return stats == null ? 0 : stats.dirtyPages * stats.pageSize;
        }

        @Override
        public int getOpenIndexes() {
            var stats = stats();
            return stats == null ? 0 : stats.openIndexes;
        }

        @Override
        public long getLockCount() {
            var stats = stats();
            return stats == null ? 0 : stats.lockCount;
        }

        @Override
        public long getCursorCount() {
            var stats = stats();
            return stats == null ? 0 : stats.cursorCount;
        }

        @Override
        public long getTransactionCount() {
            var stats = stats();
            return stats == null ? 0 : stats.transactionCount;
        }

        @Override
        public long getCheckpointDuration() {
            var stats = stats();
            return stats == null ? 0 : stats.checkpointDuration;
        }

        @Override
        public long getReplicationBacklog() {
            var stats = stats();
            return stats == null ? 0 : stats.replicationBacklog;
        }

        @Override
        public void flush() {
            asyncOp(Database::flush);
        }

        @Override
        public void sync() {
            asyncOp(Database::sync);
        }

        @Override
        public void checkpoint() {
            asyncOp(Database::checkpoint);
        }

        private Database.Stats stats() {
            synchronized (this) {
                Database.Stats stats = mStats;
                if (stats != null && (System.nanoTime() - mStatsTimestamp) < 1_000_000) { // 1ms
                    return stats;
                }
                Database db = get();
                if (db != null && !db.isClosed()) {
                    mStats = stats = db.stats();
                    mStatsTimestamp = System.nanoTime();
                    return stats;
                }
            }
            closed();
            return null;
        }

        private void asyncOp(Op op) {
            Database db = get();

            if (db == null || db.isClosed()) {
                closed();
                return;
            }

            synchronized (this) {
                if (mAsyncRunning) {
                    return;
                }

                Runner.start(() -> {
                    try {
                        op.run(db);
                    } catch (IOException e) {
                        // Ignore.
                    } finally {
                        synchronized (DbBean.this) {
                            mAsyncRunning = false;
                        }
                    }
                });

                mAsyncRunning = true;
            }
        }

        private Database db() {
            Database db = get();
            if (db != null && !db.isClosed()) {
                return db;
            }
            closed();
            return null;
        }

        private void closed() {
            clear();
            Runner.start(() -> cleanup());
        }
    }

    @FunctionalInterface
    private static interface Op {
        void run(Database db) throws IOException;
    }
}
