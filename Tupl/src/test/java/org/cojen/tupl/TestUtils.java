/*
 *  Copyright 2012 Brian S O'Neill
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
    private static final Map<Database, File> cTempDatabases = new HashMap<Database, File>();
    private static final Set<File> cTempBaseFiles = new HashSet<File>();
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

    static Database newTempDatabase() throws IOException {
        return newTempDatabase(-1);
    }

    static Database newTempDatabase(long cacheSize) throws IOException {
        DatabaseConfig config = new DatabaseConfig();
        if (cacheSize >= 0) {
            config.minCacheSize(cacheSize);
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

    static File newTempBaseFile() throws IOException {
        StackTraceElement trace = new Exception().getStackTrace()[1];
        String className = trace.getClassName();
        String prefix = "org.cojen.tupl.";
        if (className.startsWith(prefix)) {
            className = className.substring(prefix.length());
        }
        String baseName = className + '.' + trace.getMethodName();

        File tempDir = new File(System.getProperty("java.io.tmpdir"), "tupl");
        cDeleteTempDir = tempDir.exists() ? null : tempDir;
        tempDir.mkdirs();

        File baseFile = new File(tempDir, baseName);
        File lockFile = new File(tempDir, baseName + ".lock");

        int mult = 10;
        while (!lockFile.createNewFile()) {
            String suffix = "-" + (long) ((Math.random() * mult));
            baseFile = new File(tempDir, baseName + suffix);
            lockFile = new File(tempDir, baseName + suffix + ".lock");
            mult *= 10;
        }

        synchronized (cTempBaseFiles) {
            cTempBaseFiles.add(baseFile);
        }

        return baseFile;
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
        new File(baseFile.getParentFile(), baseFile.getName() + suffix).delete();
    }
}
