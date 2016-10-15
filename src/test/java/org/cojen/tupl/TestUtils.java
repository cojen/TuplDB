/*
 *  Copyright 2012-2015 Cojen.org
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
class TestUtils {
    private static final Map<Database, File> cTempDatabases = new WeakHashMap<Database, File>();
    private static final Set<File> cTempBaseFiles = new HashSet<File>();
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

    static void fastAssertArrayEquals(byte[] expected, byte[] actual) {
        if (!Arrays.equals(expected, actual)) {
            org.junit.Assert.assertArrayEquals(expected, actual);
        }
    }

    static enum OpenMode {NORMAL, DIRECT, DIRECT_MAPPED};

    static Database newTempDatabase() throws IOException {
        return newTempDatabase(-1, OpenMode.NORMAL);
    }

    static Database newTempDatabase(OpenMode mode) throws IOException {
        return newTempDatabase(-1, mode);
    }

    static Database newTempDatabase(long cacheSize) throws IOException {
        return newTempDatabase(cacheSize, OpenMode.NORMAL);
    }

    static Database newTempDatabase(long cacheSize, OpenMode mode) throws IOException {
        return newTempDatabase(cacheSize, mode, -1);
    }

    static Database newTempDatabase(long cacheSize, OpenMode mode, int checkpointRateMillis)
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
            synchronized (cTempDatabases) {
                cTempDatabases.put(db, baseFile);
            }
            return db;
        }

        return newTempDatabase(config);
    }

    static Database newTempDatabase(DatabaseConfig config) throws IOException {
        File baseFile = newTempBaseFile();
        Database db = Database.open(config.baseFile(baseFile));
        synchronized (cTempDatabases) {
            cTempDatabases.put(db, baseFile);
        }
        return db;
    }

    static File baseFileForTempDatabase(Database db) {
        synchronized (cTempDatabases) {
            return cTempDatabases.get(db);
        }
    }

    static Database reopenTempDatabase(Database db, DatabaseConfig config) throws IOException {
        return reopenTempDatabase(db, config, false);
    }

    static Database reopenTempDatabase(Database db, DatabaseConfig config, boolean deleteRedo)
        throws IOException
    {
        File baseFile;
        synchronized (cTempDatabases) {
            baseFile = cTempDatabases.remove(db);
        }
        if (baseFile == null) {
            throw new IllegalArgumentException();
        }
        db.close();

        if (deleteRedo) {
            for (File f : baseFile.getParentFile().listFiles()) {
                if (f.getName().indexOf(".redo.") > 0) {
                    f.delete();
                }
            }
        }

        db = Database.open(config.baseFile(baseFile));
        synchronized (cTempDatabases) {
            cTempDatabases.put(db, baseFile);
        }
        return db;
    }

    static File newTempBaseFile() throws IOException {
        synchronized (cTempBaseFiles) {
            if (cBaseDir == null) {
                cBaseDir = new File(System.getProperty("java.io.tmpdir"), "tupl");
                cDeleteTempDir = cBaseDir.exists() ? null : cBaseDir;
                cBaseDir.mkdirs();
            }
            File baseFile = new File
                (cBaseDir, "test-" + System.currentTimeMillis() + "-" + (++cTempId));
            cTempBaseFiles.add(baseFile);
            return baseFile;
        }
    }

    static void deleteTempDatabase(Database db) {
        if (db == null) {
            return;
        }

        try {
            db.close();
        } catch (IOException e) {
        }

        File baseFile;
        synchronized (cTempDatabases) {
            baseFile = cTempDatabases.remove(db);
        }

        if (baseFile != null) {
            deleteDbFiles(baseFile);
        }
    }

    static void deleteTempDatabases() {
        synchronized (cTempDatabases) {
            for (Database db : cTempDatabases.keySet()) {
                try {
                    db.close();
                } catch (IOException e) {
                }
            }
        }

        synchronized (cTempBaseFiles) {
            for (File baseFile : cTempBaseFiles) {
                deleteDbFiles(baseFile);
            }
            cTempBaseFiles.clear();
        }
    }

    static void deleteRecursively(File file) {
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

    static byte[] randomStr(Random rnd, int size) {
        return randomStr(rnd, null, size, size);
    }

    static byte[] randomStr(Random rnd, int min, int max) {
        return randomStr(rnd, null, min, max);
    }

    static byte[] randomStr(Random rnd, byte[] prefix, int min, int max) {
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

    static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
        }
    }

    static boolean is64bit() {
        return "amd64".equals(System.getProperty("os.arch"))
            || "64".equals(System.getProperty("sun.arch.data.model"));
    }

    private static volatile Object cForceGcRef;

    static void forceGc() {
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

    static synchronized File findSourceDirectory() throws IOException {
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

    static File findSourceDirectory(Set<File> visited, File dir, int matchDepth) {
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
}
