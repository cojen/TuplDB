/*
 *  Copyright 2012-2013 Brian S O'Neill
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

    static Database newTempDatabase() throws IOException {
        return newTempDatabase(-1);
    }

    static Database newTempDatabase(long cacheSize) throws IOException {
        DatabaseConfig config = new DatabaseConfig();
        if (cacheSize >= 0) {
            config.minCacheSize(cacheSize);
        }
        config.durabilityMode(DurabilityMode.NO_FLUSH);
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

    private static void deleteDbFiles(File baseFile) {
        deleteDbFile(baseFile, ".db");
        deleteDbFile(baseFile, ".info");
        deleteDbFile(baseFile, ".lock");
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
}
