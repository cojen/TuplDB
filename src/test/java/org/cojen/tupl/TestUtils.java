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
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.cojen.tupl.io.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
@org.junit.Ignore
public class TestUtils {
    private static final Map<Class, TempFiles> cTempFiles = new HashMap<>();

    private static long cTempId;
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

    public static void fastAssertArrayEquals(byte[] expected, byte[] actual) {
        if (!Arrays.equals(expected, actual)) {
            org.junit.Assert.assertArrayEquals(expected, actual);
        }
    }

    static enum OpenMode {NORMAL, DIRECT, DIRECT_MAPPED};

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

    public static File baseFileForTempDatabase(Class context, Database db) {
        return tempFilesFor(context).baseFileForTempDatabase(db);
    }

    public static Database reopenTempDatabase(Class context, Database db, DatabaseConfig config)
        throws IOException
    {
        return reopenTempDatabase(context, db, config, false);
    }

    public static Database reopenTempDatabase(Class context, Database db,
                                              DatabaseConfig config, boolean deleteRedo)
        throws IOException
    {
        return tempFilesFor(context).reopenTempDatabase(db, config, deleteRedo);
    }

    public static File newTempBaseFile(Class context) throws IOException {
        return tempFilesFor(context).newTempBaseFile();
    }

    public static void deleteTempDatabase(Class context, Database db) {
        tempFilesFor(context).deleteTempDatabase(db);
    }

    public static void deleteTempDatabases(Class context) {
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

    public static void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] dirs = file.listFiles();
            if (dirs != null) {
                for (File dir : dirs) {
                    deleteRecursively(dir);
                }
            }
        }

        if (file.exists()) {
            file.delete();
        }
    }

    private static void deleteDbFiles(File baseFile) {
        deleteDbFile(baseFile, ".db");
        deleteDbFile(baseFile, ".info");
        deleteDbFile(baseFile, ".lock");
        deleteDbFile(baseFile, ".primer");
        try {
            Utils.deleteNumberedFiles(baseFile, ".redo.");
        } catch (IOException e) {
        }
    }

    private static void deleteDbFile(File baseFile, String suffix) {
        File f = new File(baseFile.getParentFile(), baseFile.getName() + suffix);
        f.delete();
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
        t.start();
        while (true) {
            Thread.State state = t.getState();
            if (state != Thread.State.NEW && state != Thread.State.RUNNABLE) {
                return t;
            }
            Thread.yield();
        }
    }

    public static boolean is64bit() {
        return "amd64".equals(System.getProperty("os.arch"))
            || "64".equals(System.getProperty("sun.arch.data.model"));
    }

    private static volatile Object cForceGcRef;

    public static void forceGc() {
        for (int x=0; x<10; x++) {
            System.gc();
        }
        List<String> list = new ArrayList<String>();
        for (int x=0; x<1000; x++) {
            list.add("" + x);
        }
        cForceGcRef = list;
        for (int x=0; x<10; x++) {
            System.gc();
        }
        cForceGcRef = null;
    }

    private static File cSourceDir;

    public static synchronized File findSourceDirectory() throws IOException {
        if (cSourceDir != null) {
            return cSourceDir;
        }

        Set<File> visited = new HashSet<>();
        File dir = new File(System.getProperty("user.dir")).getAbsoluteFile();
        if (!dir.isDirectory()) {
            throw new IllegalStateException("Not a directory: " + dir);
        }

        do {
            File found = findSourceDirectory(visited, dir, 0);
            if (found != null) {
                cSourceDir = found;
                return found;
            }
        } while ((dir = dir.getParentFile()) != null);

        return null;
    }

    public static File findSourceDirectory(Set<File> visited, File dir, int matchDepth) {
        if (!visited.add(dir)) {
            return null;
        }

        String match;
        boolean tail = false;

        switch (matchDepth) {
        case 0:
            match = "org";
            break;
        case 1:
            match = "cojen";
            break;
        case 2:
            match = "tupl";
            break;
        case 3:
            match = "LocalDatabase.java";
            tail = true;
            break;
        default:
            throw new IllegalStateException();
        }

        File file = new File(dir, match);

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

    private static synchronized File createTempBaseFile(String prefix) throws IOException {
        if (cBaseDir == null) {
            cBaseDir = new File(System.getProperty("java.io.tmpdir"), "tupl");
            cDeleteTempDir = cBaseDir.exists() ? null : cBaseDir;
            cBaseDir.mkdirs();
        }
        return new File(cBaseDir, prefix + "-" + System.currentTimeMillis() + "-" + (++cTempId));
    }

    static class TempFiles {
        private final Map<Database, File> mTempDatabases = new WeakHashMap<>();
        private final Set<File> mTempBaseFiles = new HashSet<>();

        private final String mPrefix;

        TempFiles(String prefix) {
            mPrefix = prefix;
        }

        Database newTempDatabase(long cacheSize, OpenMode mode, int checkpointRateMillis)
            throws IOException
        {
            DatabaseConfig config = new DatabaseConfig();
            if (cacheSize >= 0) {
                config.minCacheSize(cacheSize);
            }
            config.durabilityMode(DurabilityMode.NO_FLUSH);
            config.directPageAccess(false);

            if (checkpointRateMillis >= 0) {
                config.checkpointRate(checkpointRateMillis, TimeUnit.MILLISECONDS);
            }

            switch (mode) {
            default:
                throw new IllegalArgumentException();
            case NORMAL:
                config.directPageAccess(false);
                break;
            case DIRECT:
                config.directPageAccess(true);
                break;
            case DIRECT_MAPPED:
                int pageSize = config.mPageSize;
                if (pageSize == 0) {
                    pageSize = 4096;
                }
                if (cacheSize < 0) {
                    cacheSize = pageSize * 1000;
                }
                File baseFile = newTempBaseFile();
                config.baseFile(baseFile);
                File dbFile = new File(baseFile.getParentFile(), baseFile.getName() + ".db");
                MappedPageArray pa = MappedPageArray.open
                    (pageSize, (cacheSize + pageSize - 1) / pageSize, dbFile,
                     EnumSet.of(OpenOption.CREATE, OpenOption.MAPPED));
                config.dataPageArray(pa);
                config.directPageAccess(true);
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

        synchronized File baseFileForTempDatabase(Database db) {
            return mTempDatabases.get(db);
        }

        Database reopenTempDatabase(Database db, DatabaseConfig config, boolean deleteRedo)
            throws IOException
        {
            File baseFile;
            synchronized (this) {
                baseFile = mTempDatabases.remove(db);
            }
            if (baseFile == null) {
                throw new IllegalArgumentException();
            }
            db.close();

            if (deleteRedo) {
                String baseName = baseFile.getName();
                for (File f : baseFile.getParentFile().listFiles()) {
                    String name = f.getName();
                    if (name.startsWith(baseName) && name.indexOf(".redo.") > 0) {
                        f.delete();
                    }
                }
            }

            db = Database.open(config.baseFile(baseFile));
            synchronized (this) {
                mTempDatabases.put(db, baseFile);
            }
            return db;
        }

        synchronized File newTempBaseFile() throws IOException {
            File baseFile = TestUtils.createTempBaseFile(mPrefix);
            mTempBaseFiles.add(baseFile);
            return baseFile;
        }

        void deleteTempDatabase(Database db) {
            if (db == null) {
                return;
            }

            try {
                db.close();
            } catch (IOException e) {
            }

            File baseFile;
            synchronized (this) {
                baseFile = mTempDatabases.remove(db);
                mTempBaseFiles.remove(baseFile);
            }
            
            if (baseFile != null) {
                deleteDbFiles(baseFile);
            }
        }

        synchronized void deleteTempDatabases() {
            for (Database db : mTempDatabases.keySet()) {
                try {
                    db.close();
                } catch (IOException e) {
                }
            }

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
