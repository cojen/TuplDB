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

import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;

import javax.management.JMException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.cojen.tupl.Database;

import org.cojen.tupl.diag.DatabaseStats;
import org.cojen.tupl.diag.VerificationObserver;

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
            var bean = new DbBeanWrapper(db, base);
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
            if (ref instanceof DbBean bean) {
                doUnregister(bean.mBase);
            }
        }
    }

    private static String sanitize(String base) {
        return base.replace(',', '_').replace('=', '_').replace(':', '_').replace('"', '_');
    }

    private static ObjectName newObjectName(String base) throws JMException {
        return new ObjectName("org.cojen.tupl", "database", base);
    }

    private static record Listener(NotificationListener listener,
                                   NotificationFilter filter, Object handback) { }

    private static class DbBeanWrapper extends StandardMBean implements NotificationEmitter {
        DbBeanWrapper(Database db, String base) throws NotCompliantMBeanException {
            super(new DbBean(db, base), DatabaseMBean.class);
            dbBean().mSource = this;
        }

        @Override
        public void addNotificationListener(NotificationListener listener,
                                            NotificationFilter filter, Object handback)
        {
            dbBean().addNotificationListener(listener, filter, handback);
        }

        @Override
        public void removeNotificationListener(NotificationListener listener)
            throws ListenerNotFoundException
        {
            dbBean().removeNotificationListener(listener);
        }

        @Override
        public void removeNotificationListener(NotificationListener listener,
                                               NotificationFilter filter, Object handback)
            throws ListenerNotFoundException
        {
            dbBean().removeNotificationListener(listener, filter, handback);
        }

        @Override
        public MBeanNotificationInfo[] getNotificationInfo() {
            return dbBean().getNotificationInfo();
        }

        private DbBean dbBean() {
            return (DbBean) getImplementation();
        }
    }

    private static class DbBean extends WeakReference<Database>
        implements DatabaseMBean, NotificationEmitter
    {
        private final String mBase;
        private Object mSource;

        private DatabaseStats mStats;
        private long mStatsTimestamp;

        private boolean mAsyncRunning;

        private final Map<Listener, Boolean> mListeners = new ConcurrentHashMap<>(2);

        private long mSequenceNumber;

        DbBean(Database db, String base) {
            super(db);
            mBase = base;
        }

        @Override
        public void addNotificationListener(NotificationListener listener,
                                            NotificationFilter filter, Object handback)
        {
            mListeners.put(new Listener(listener, filter, handback), true);
        }

        @Override
        public void removeNotificationListener(NotificationListener listener)
            throws ListenerNotFoundException
        {
            removeNotificationListener(listener, null, null);
        }

        @Override
        public void removeNotificationListener(NotificationListener listener,
                                               NotificationFilter filter, Object handback)
            throws ListenerNotFoundException
        {
            if (!mListeners.remove(new Listener(listener, filter, handback))) {
                throw new ListenerNotFoundException();
            }
        }

        @Override
        public MBeanNotificationInfo[] getNotificationInfo() {
            return new MBeanNotificationInfo[0];
        }

        private Notification newNotification(String type, String message) {
            return newNotification(type, message, null);
        }

        private Notification newNotification(String type, Throwable ex) {
            return newNotification(type, ex.toString());
        }

        private Notification newNotification(String type, String message, Object param) {
            String prefix = "Database." + type;
            if (param != null) {
                prefix = prefix + '(' + param + ')';
            }
            message = prefix + ' ' + message;

            type = Database.class.getName() + '.' + type;

            long num;
            synchronized (this) {
                num = ++mSequenceNumber;
            }

            return new Notification(type, mSource, num, System.currentTimeMillis(), message);
        }

        private void notifyAll(Notification n) {
            for (Listener listener : mListeners.keySet()) {
                if (listener.filter == null || listener.filter.isNotificationEnabled(n)) {
                    try {
                        listener.listener.handleNotification(n, listener.handback);
                    } catch (Throwable e) {
                        Utils.uncaught(e);
                    }
                }
            }
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
        public boolean isLeader() {
            Database db = db();
            return db == null ? false : db.isLeader();
        }

        @Override
        public void flush() {
            asyncOp(db -> {
                try {
                    db.flush();
                    return newNotification("flush", "finished");
                } catch (Throwable e) {
                    return newNotification("flush", e);
                }
            });
        }

        @Override
        public void sync() {
            asyncOp(db -> {
                try {
                    db.sync();
                    return newNotification("sync", "finished");
                } catch (Throwable e) {
                    return newNotification("sync", e);
                }
            });
        }

        @Override
        public void checkpoint() {
            asyncOp(db -> {
                try {
                    db.checkpoint();
                    return newNotification("checkpoint", "finished");
                } catch (Throwable e) {
                    return newNotification("checkpoint", e);
                }
            });
        }

        @Override
        public void compactFile(double target) {
            if (target < 0 || target > 1) {
                throw new IllegalArgumentException("Illegal compaction target: " + target);
            }
            asyncOp(db -> {
                try {
                    boolean result = db.compactFile(null, target);
                    return newNotification("compactFile", result ? "finished" : "aborted", target);
                } catch (Throwable e) {
                    return newNotification("compactFile", e);
                }
            });
        }

        @Override
        public void verify() {
            asyncOp(db -> {
                try {
                    return newNotification("verify", doVerify(db));
                } catch (Throwable e) {
                    return newNotification("verify", e);
                }
            });
        }

        private String doVerify(Database db) throws IOException {
            var observer = new VerificationObserver() {
                volatile String failed;

                @Override
                public boolean indexNodeFailed(long id, int level, String message) {
                    var b = new StringBuilder("failed: ");
                    appendFailedMessage(b, id, level, message);
                    failed = b.toString();
                    return false;
                }
            };

            if (db.verify(observer, 0)) {
                return "passed";
            } else {
                String message = observer.failed;
                if (message == null) {
                    message = "failed";
                }
                return message;
            }
        }

        @Override
        public boolean failover() {
            try {
                Database db = db();
                return db == null ? false : db.failover();
            } catch (IOException e) {
                return false;
            }
        }

        private DatabaseStats stats() {
            synchronized (this) {
                DatabaseStats stats = mStats;
                if (stats != null && (System.nanoTime() - mStatsTimestamp) < 1_000_000) { // 1ms
                    return stats;
                }
                Database db = db();
                if (db != null) {
                    mStats = stats = db.stats();
                    mStatsTimestamp = System.nanoTime();
                    return stats;
                }
            }
            return null;
        }

        private void asyncOp(Op op) {
            Database db = db();

            if (db == null) {
                throw new IllegalStateException("Database is closed");
            }

            synchronized (this) {
                if (mAsyncRunning) {
                    throw new IllegalStateException("Another operation is in progress");
                }

                Runner.start(() -> {
                    Notification n;
                    try {
                        n = op.run(db);
                    } finally {
                        synchronized (DbBean.this) {
                            mAsyncRunning = false;
                        }
                    }

                    if (n != null) {
                        notifyAll(n);
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
            clear();
            Runner.start(() -> cleanup());
            return null;
        }
    }

    @FunctionalInterface
    private static interface Op {
        Notification run(Database db);
    }
}
