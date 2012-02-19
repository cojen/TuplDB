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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class TempFileManager {
    private File mBaseFile;
    private long mCount;
    private Map<File, Closeable> mFiles;

    TempFileManager(File baseFile) throws IOException {
        mBaseFile = baseFile;

        // Clean up old files.
        String prefix = baseFile.getName() + ".temp.";
        for (File file : baseFile.getParentFile().listFiles()) {
            String name = file.getName();
            if (name.startsWith(prefix)) {
                String suffix = name.substring(prefix.length());
                try {
                    Long.parseLong(suffix);
                } catch (NumberFormatException e) {
                    continue;
                }
                file.delete();
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                shutdown();
            }
        });
    }

    File createTempFile() throws IOException {
        while (true) {
            File file;
            synchronized (this) {
                if (mBaseFile == null) {
                    throw new IOException("Shutting down");
                }
                file = new File(mBaseFile.getPath() + ".temp." + (mCount++));
                if (mFiles == null) {
                    mFiles = new HashMap<File, Closeable>(4);
                }
                if (mFiles.containsKey(file)) {
                    continue;
                }
                mFiles.put(file, null);
            }
            if (file.createNewFile()) {
                // Note: File.deleteOnExit should never be used, since it leaks memory.
                return file;
            }
            synchronized (this) {
                mFiles.remove(file);
            }
        }
    }

    synchronized void register(File file, Closeable c) throws IOException {
        if (mFiles == null || !mFiles.containsKey(file)) {
            if (mBaseFile == null) {
                throw new IOException("Shutting down");
            }
            return;
        }
        if (mFiles.get(file) != null) {
            // Only one registration allowed.
            throw new IllegalStateException();
        }
        mFiles.put(file, c);
    }

    void deleteTempFile(File file) {
        Closeable c;
        synchronized (this) {
            if (mFiles == null || !mFiles.containsKey(file)) {
                return;
            }
            c = mFiles.get(file);
        }
        if (c != null) {
            try {
                c.close();
            } catch (IOException e) {
                // Ignore.
            }
        }
        file.delete();
    }

    public synchronized void shutdown() {
        mBaseFile = null;
        if (mFiles != null) {
            for (Map.Entry<File, Closeable> entry : mFiles.entrySet()) {
                Closeable c = entry.getValue();
                if (c != null) {
                    try {
                        c.close();
                    } catch (IOException e) {
                        // Ignore.
                    }
                }
                entry.getKey().delete();
            }
            mFiles.clear();
        }
    }
}
