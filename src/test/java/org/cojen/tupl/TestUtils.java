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

package org.cojen.tupl;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.cojen.tupl.core.Utils;

import org.cojen.tupl.io.MappedPageArray;
import org.cojen.tupl.io.OpenOption;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.Runner;

import org.junit.Assert;

/**
 * 
 *
 * @author Brian S O'Neill
 */
@org.junit.Ignore
public class TestUtils {
    private static final Map<Class, TempFiles> cTempFiles = new HashMap<>();

    private static File cBaseDir;
    private static volatile File cDeleteTempDir;

    static {
        // Force this class to be loaded early, to avoid shutdown hook loading
        // it later. It can cause problems with coverage frameworks which
        // attempt to install a shutdown hook, and then fail.
        Utils.roundUpPower2(1);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                /*
                if (cShutdownHookEnabled) {
                    deleteTempDatabases();
                }
                */
                if (cDeleteTempDir != null) {
                    cDeleteTempDir.delete();
                }
            }
        });
    }

    public static void assertNativeAccessEnabled() {
        if (!Utils.isNativeAccessEnabled(TestUtils.class.getModule())) {
            throw new AssertionError("Native access isn't enabled for module");
        }
    }

    public static void fastAssertArrayEquals(byte[] expected, byte[] actual) {
        if (!Arrays.equals(expected, actual)) {
            org.junit.Assert.assertArrayEquals(expected, actual);
        }
    }

    public static void waitToBecomeLeader(Database db, int seconds) throws InterruptedException {
        var latch = new Latch(Latch.EXCLUSIVE);
        db.uponLeader(latch::releaseExclusive, null);
        Assert.assertTrue(latch.tryAcquireExclusiveNanos(seconds * 1_000_000_000L));
    }

    public static enum OpenMode {NORMAL, MAPPED}

    public static Database newTempDatabase(Class context) throws IOException {
        return newTempDatabase(context, -1, OpenMode.NORMAL);
    }

    public static Database newTempDatabase(Class context, OpenMode mode) throws IOException {
        return newTempDatabase(context, -1, mode);
    }

    public static Database newTempDatabase(Class context, long cacheSize) throws IOException {
        return newTempDatabase(context, cacheSize, OpenMode.NORMAL);
    }

    public static Database newTempDatabase(Class context, long cacheSize, OpenMode mode)
        throws IOException
    {
        return newTempDatabase(context, cacheSize, mode, -1);
    }

    public static Database newTempDatabase(Class context, long cacheSize, OpenMode mode,
                                           int checkpointRateMillis)
        throws IOException
    {
        return tempFilesFor(context).newTempDatabase(cacheSize, mode, checkpointRateMillis);
    }

    public static Database newTempDatabase(Class context, DatabaseConfig config)
        throws IOException
    {
        return tempFilesFor(context).newTempDatabase(config);
    }

    public static Database copyTempDatabase(Class context, Database db, DatabaseConfig config)
        throws IOException
    {
        return tempFilesFor(context).copyTempDatabase(db, config);
    }

    public static File baseFileForTempDatabase(Class context, Database db) {
        return tempFilesFor(context).baseFileForTempDatabase(db);
    }

    public static Database reopenTempDatabase(Class context, Database db, DatabaseConfig config)
        throws IOException
    {
        return tempFilesFor(context).reopenTempDatabase(db, config, false, false);
    }

    public static Database reopenTempDatabase(Class context, Database db,
                                              Function<File, DatabaseConfig> configFactory)
        throws IOException
    {
        return tempFilesFor(context).reopenTempDatabase(db, configFactory, false, false);
    }

    public static Database reopenTempDatabase(Class context, Database db,
                                              DatabaseConfig config, boolean deleteRedo)
        throws IOException
    {
        return tempFilesFor(context).reopenTempDatabase(db, config, deleteRedo, false);
    }

    public static Database reopenTempDatabase(Class context, Database db,
                                              Function<File, DatabaseConfig> configFactory,
                                              boolean deleteRedo)
        throws IOException
    {
        return tempFilesFor(context).reopenTempDatabase(db, configFactory, deleteRedo, false);
    }

    public static Database destroyTempDatabase(Class context, Database db, DatabaseConfig config)
        throws IOException
    {
        return tempFilesFor(context).reopenTempDatabase(db, config, false, true);
    }

    public static Database destroyTempDatabase(Class context, Database db,
                                               Function<File, DatabaseConfig> configFactory)
        throws IOException
    {
        return tempFilesFor(context).reopenTempDatabase(db, configFactory, false, true);
    }

    public static File newTempBaseFile(Class context) throws IOException {
        return tempFilesFor(context).newTempBaseFile();
    }

    public static void deleteTempDatabase(Class context, Database db) throws IOException {
        tempFilesFor(context).deleteTempDatabase(db);
    }

    public static void deleteTempDatabases(Class context) throws IOException {
        TempFiles files = removeTempFilesFor(context);
        if (files != null) {
            files.deleteTempDatabases();
        }
    }

    public static void deleteTempFiles(Class context) {
        TempFiles files = removeTempFilesFor(context);
        if (files != null) {
            files.deleteTempFiles();
        }
    }

    public static void closeTempDatabases(Class context) throws IOException {
        TempFiles files;
        synchronized (cTempFiles) {
            files = cTempFiles.get(context);
        }
        if (files != null) {
            files.closeTempDatabases();
        }
    }

    private static synchronized TempFiles tempFilesFor(Class context) {
        TempFiles files = cTempFiles.get(context);
        if (files == null) {
            files = new TempFiles(context.getSimpleName());
            cTempFiles.put(context, files);
        }
        return files;
    }

    private static synchronized TempFiles removeTempFilesFor(Class context) {
        return cTempFiles.remove(context);
    }

    public static void deleteRecursively(File file) throws IOException {
        if (file.isDirectory()) {
            File[] dirs = file.listFiles();
            if (dirs != null) {
                for (File dir : dirs) {
                    deleteRecursively(dir);
                }
            }
        }

        Utils.delete(file);
    }

    private static void deleteDbFiles(File baseFile) throws IOException {
        deleteDbFile(baseFile, ".db");
        deleteDbFile(baseFile, ".lock");
        deleteDbFile(baseFile, ".primer");
        Utils.deleteNumberedFiles(baseFile, ".redo.");
    }

    private static void deleteDbFile(File baseFile, String suffix) throws IOException {
        var f = new File(baseFile.getParentFile(), baseFile.getName() + suffix);
        Utils.delete(f);
    }

    public static byte[] randomStr(Random rnd, int size) {
        return randomStr(rnd, null, size, size);
    }

    public static byte[] randomStr(Random rnd, int min, int max) {
        return randomStr(rnd, null, min, max);
    }

    public static byte[] randomStr(Random rnd, byte[] prefix, int min, int max) {
        int size;
        if (min == max) {
            size = max;
        } else {
            size = min + rnd.nextInt(max - min);
        }

        int i;
        byte[] str;
        if (prefix == null) {
            i = 0;
            str = new byte[size];
        } else {
            i = prefix.length;
            str = new byte[i + size];
            System.arraycopy(prefix, 0, str, 0, i);
        }

        // Fill with printable ascii characters.
        for (; i < str.length; i++) {
            str[i] = (byte) (33 + rnd.nextInt(127 - 33));
        }

        return str;
    }

    public static void sleep(long millis) {
        if (millis <= 0) {
            if (millis < 0) {
                throw new IllegalArgumentException("" + millis);
            }
            return;
        }

        long end = System.currentTimeMillis() + millis;
        do {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
            }
        } while ((millis = end - System.currentTimeMillis()) > 0);
    }

    public static <T extends Thread> T startAndWaitUntilBlocked(T t) {
        return startAndWaitUntilBlocked(t, (String) null, null);
    }

    public static <T extends Thread> T startAndWaitUntilBlocked(T t, Class where, String method) {
        String whereName = where == null ? null : where.getName();
        return startAndWaitUntilBlocked(t, whereName, method);
    }

    public static <T extends Thread> T startAndWaitUntilBlocked(T t, String where, String method) {
        t.start();
        while (true) {
            Thread.State state = t.getState();

            if (state != Thread.State.NEW && state != Thread.State.RUNNABLE) {
                if (where == null) {
                    return t;
                }

                for (var trace : t.getStackTrace()) {
                    if (where.equals(trace.getClassName())) {
                        if (method == null || method.equals(trace.getMethodName())) {
                            return t;
                        }
                    }
                }
            }

            if (state == Thread.State.TERMINATED) {
                throw new IllegalStateException("Thread terminated early: " + t);
            }

            Thread.yield();
        }
    }

    public static <T extends Thread> T startAndWaitUntilBlockedSocket(T t)
        throws InterruptedException
    {
        t.start();

        StackTraceElement[] lastTrace = null;

        while (true) {
            Thread.State state = t.getState();
            if (state != Thread.State.NEW && state != Thread.State.RUNNABLE) {
                return t;
            }

            // A thread which is blocked reading from a Socket reports a RUNNABLE state. Need
            // to inspect the stack trace instead.

            StackTraceElement[] trace = t.getStackTrace();
            if (Arrays.equals(trace, lastTrace) && trace.length != 0) {
                StackTraceElement top = trace[0];
                if (top.isNativeMethod()
                    && top.getClassName().contains("Socket")
                    && top.getMethodName().contains("read"))
                {
                    return t;
                }
            }

            lastTrace = trace;

            Thread.sleep(100);
        }
    }

    /**
     * Returns a task which when joined, re-throws any exception from the task.
     */
    public static <T extends Runnable> TestTask<T> startTestTask(T task) {
        var tt = new TestTask<>(task);
        Runner.start(tt);
        return tt;
    }

    /**
     * Returns a task which when joined, re-throws any exception from the task.
     */
    public static <T extends Runnable> TestTask<T> startTestTaskAndWaitUntilBlocked(T task) {
        var tt = new TestTask<>(task);
        startAndWaitUntilBlocked(new Thread(tt));
        return tt;
    }

    /**
     * Returns a task which when joined, re-throws any exception from the task.
     */
    public static <T extends Runnable> TestTask<T> startTestTaskAndWaitUntilBlockedSocket(T task)
        throws InterruptedException
    {
        var tt = new TestTask<>(task);
        startAndWaitUntilBlockedSocket(new Thread(tt));
        return tt;
    }

    public static class TestTask<T extends Runnable> implements Runnable {
        private final T mTask;
        private boolean mFinished;
        private Throwable mException;

        TestTask(T task) {
            mTask = task;
        }

        public synchronized void join() throws InterruptedException {
            while (!mFinished) {
                wait();
            }
            if (mException != null) {
                throw Utils.rethrow(mException);
            }
        }

        @Override
        public synchronized void run() {
            if (mFinished) {
                throw new IllegalStateException();
            }
            try {
                mTask.run();
            } catch (Throwable e) {
                mException = e;
            } finally {
                mFinished = true;
                notifyAll();
            }
        }
    }

    public static void assume64bit() {
        org.junit.Assume.assumeTrue(is64bit());
    }

    public static boolean is64bit() {
        return "amd64".equals(System.getProperty("os.arch"))
            || "64".equals(System.getProperty("sun.arch.data.model"));
    }

    public static ServerSocket newServerSocket() throws IOException {
        return new ServerSocket(0, 1000, InetAddress.getLoopbackAddress());
    }

    private static File cSourceDir;

    public static synchronized File findSourceDirectory() {
        if (cSourceDir != null) {
            return cSourceDir;
        }

        var visited = new HashSet<File>();
        var dir = new File(System.getProperty("user.dir")).getAbsoluteFile();
        if (!dir.isDirectory()) {
            throw new IllegalStateException("Not a directory: " + dir);
        }

        while (true) {
            File parent = dir.getParentFile();
            if (parent == null) {
                // Don't search the root directory.
                break;
            }
            File found = findSourceDirectory(visited, dir, 0);
            if (found != null) {
                cSourceDir = found;
                return found;
            }
            dir = parent;
        }

        return null;
    }

    public static File findSourceDirectory(Set<File> visited, File dir, int matchDepth) {
        String match;
        boolean tail = false;

        switch (matchDepth) {
            case 0 -> match = "org";
            case 1 -> match = "cojen";
            case 2 -> match = "tupl";
            case 3 -> match = "core";
            case 4 -> {
                match = "LocalDatabase.java";
                tail = true;
            }
            default -> throw new IllegalStateException();
        }

        var file = new File(dir, match);

        if (!visited.add(file)) {
            return null;
        }

        if (file.exists()) {
            if (!file.isDirectory()) {
                if (tail) {
                    return dir;
                }
            } else if (!tail) {
                // Search down.
                File found = findSourceDirectory(visited, file, matchDepth + 1);
                if (found != null) {
                    return found;
                }
            }
        }

        // Search peers.

        File[] peerDirs = dir.listFiles(f -> f.isDirectory() && !f.isHidden());
        if (peerDirs != null) {
            for (File peer : peerDirs) {
                File found = findSourceDirectory(visited, peer, 0);
                if (found != null) {
                    return found;
                }
            }
        }

        return null;
    }

    private static synchronized File createTempBaseFile(String prefix) {
        if (cBaseDir == null) {
            cBaseDir = new File(System.getProperty("java.io.tmpdir"), "tupl");
            cDeleteTempDir = cBaseDir.exists() ? null : cBaseDir;
            cBaseDir.mkdirs();
        }
        return new File(cBaseDir, prefix + "-" + UUID.randomUUID());
    }

    static class TempFiles {
        private final Map<Database, File> mTempDatabases = new HashMap<>();
        private final Set<File> mTempBaseFiles = new HashSet<>();

        private final String mPrefix;

        TempFiles(String prefix) {
            mPrefix = prefix;
        }

        Database newTempDatabase(long cacheSize, OpenMode mode, int checkpointRateMillis)
            throws IOException
        {
            var config = new DatabaseConfig();
            if (cacheSize >= 0) {
                config.minCacheSize(cacheSize);
            }
            config.durabilityMode(DurabilityMode.NO_FLUSH);

            if (checkpointRateMillis >= 0) {
                config.checkpointRate(checkpointRateMillis, TimeUnit.MILLISECONDS);
            }

            if (mode == OpenMode.MAPPED) {
                org.junit.Assume.assumeTrue(MappedPageArray.isSupported());
                int pageSize = 4096;
                if (cacheSize < 0) {
                    cacheSize = pageSize * 1000;
                }
                File baseFile = newTempBaseFile();
                config.baseFile(baseFile);
                var dbFile = new File(baseFile.getParentFile(), baseFile.getName() + ".db");
                Supplier<MappedPageArray> factory = MappedPageArray.factory
                    (pageSize, (cacheSize + pageSize - 1) / pageSize, dbFile,
                     EnumSet.of(OpenOption.CREATE, OpenOption.MAPPED));
                config.dataPageArray(factory);
                Database db = Database.open(config);
                synchronized (this) {
                    mTempDatabases.put(db, baseFile);
                }
                return db;
            }

            return newTempDatabase(config);
        }

        Database newTempDatabase(DatabaseConfig config) throws IOException {
            File baseFile = newTempBaseFile();
            Database db = Database.open(config.baseFile(baseFile));
            synchronized (this) {
                mTempDatabases.put(db, baseFile);
            }
            return db;
        }

        Database copyTempDatabase(Database db, DatabaseConfig config) throws IOException {
            File srcBaseFile = baseFileForTempDatabase(db);
            File dstBaseFile = newTempBaseFile();

            File srcParentFile = srcBaseFile.getParentFile();
            String baseName = srcBaseFile.getName();
            for (File src : srcParentFile.listFiles()) {
                String name = src.getName();
                int ix = name.indexOf('.');
                if (ix < 0) {
                    continue;
                }
                String extension = name.substring(ix);
                if (name.startsWith(baseName) && !extension.equals(".lock")) {
                    File dst = new File(srcParentFile, dstBaseFile.getName() + extension);
                    try (var in = new FileInputStream(src)) {
                        try (var out = new FileOutputStream(dst)) {
                            in.transferTo(out);
                        }
                    }
                }
            }

            db = Database.open(config.baseFile(dstBaseFile));

            synchronized (this) {
                mTempDatabases.put(db, dstBaseFile);
            }

            return db;
        }

        synchronized File baseFileForTempDatabase(Database db) {
            return mTempDatabases.get(db);
        }

        Database reopenTempDatabase(Database db, DatabaseConfig config,
                                    boolean deleteRedo, boolean destroy)
            throws IOException
        {
            return reopenTempDatabase(db, (file) -> config.baseFile(file), deleteRedo, destroy);
        }

        Database reopenTempDatabase(Database db, Function<File, DatabaseConfig> configFactory,
                                    boolean deleteRedo, boolean destroy)
            throws IOException
        {
            File baseFile;
            synchronized (this) {
                baseFile = mTempDatabases.remove(db);
            }
            db.close();

            if (deleteRedo && baseFile != null) {
                String baseName = baseFile.getName();
                for (File f : baseFile.getParentFile().listFiles()) {
                    String name = f.getName();
                    if (name.startsWith(baseName) && name.indexOf(".redo.") > 0) {
                        f.delete();
                    }
                }
            }

            DatabaseConfig config = configFactory.apply(baseFile);

            if (destroy) {
                db = Database.destroy(config);
            } else {
                db = Database.open(config);
            }

            synchronized (this) {
                mTempDatabases.put(db, baseFile);
            }
            return db;
        }

        synchronized File newTempBaseFile() {
            File baseFile = TestUtils.createTempBaseFile(mPrefix);
            mTempBaseFiles.add(baseFile);
            return baseFile;
        }

        void deleteTempDatabase(Database db) throws IOException {
            if (db == null) {
                return;
            }

            db.close();

            File baseFile;
            synchronized (this) {
                baseFile = mTempDatabases.remove(db);
                mTempBaseFiles.remove(baseFile);
            }
            
            if (baseFile != null) {
                deleteDbFiles(baseFile);
            }
        }

        synchronized void closeTempDatabases() throws IOException {
            for (Database db : mTempDatabases.keySet()) {
                db.close();
            }
        }

        synchronized void deleteTempDatabases() throws IOException {
            closeTempDatabases();

            for (File baseFile : mTempBaseFiles) {
                deleteDbFiles(baseFile);
            }

            mTempDatabases.clear();
            mTempBaseFiles.clear();
        }

        synchronized void deleteTempFiles() {
            for (File baseFile : mTempBaseFiles) {
                String prefix = baseFile.getName();
                baseFile.getParentFile().listFiles(file -> {
                    if (file.getName().startsWith(prefix)) {
                        file.delete();
                    }
                    return false;
                });
            }

            mTempBaseFiles.clear();
        }
    }
}
