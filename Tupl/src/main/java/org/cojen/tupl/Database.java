/*
 *  Copyright 2011 Brian S O'Neill
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

import java.util.concurrent.locks.Lock;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class Database implements Closeable {
    private static final int DEFAULT_CACHED_NODES = 1000;
    private static final byte DB_TYPE_USER = 0;

    private final TreeNodeStore mNodeStore;

    public Database(DatabaseConfig config) throws IOException {
        File baseFile = config.mBaseFile;
        if (baseFile == null) {
            throw new IllegalArgumentException("No base file provided");
        }
        if (baseFile.isDirectory()) {
            throw new IllegalArgumentException("Base file is a directory: " + baseFile);
        }

        String basePath = baseFile.getPath();
        File file0 = new File(basePath + ".0.db");
        File file1 = new File(basePath + ".1.db");

        DualFilePageStore pageStore;
        if (config.mPageSize <= 0) {
            pageStore = new DualFilePageStore(file0, file1, config.mReadOnly);
        } else {
            pageStore = new DualFilePageStore(file0, file1, config.mReadOnly, config.mPageSize);
        }

        int minCache = config.mMinCache;
        int maxCache = config.mMaxCache;

        if (maxCache == 0) {
            maxCache = minCache;
            if (maxCache == 0) {
                minCache = maxCache = DEFAULT_CACHED_NODES;
            }
        }

        mNodeStore = new TreeNodeStore(pageStore, minCache, maxCache);
    }

    /**
     * Returns a full view into the given named sub database, creating it if
     * necessary.
     */
    public OrderedView openOrderedView(byte[] name) throws IOException {
        byte[] nameKey = new byte[1 + name.length];
        nameKey[0] = DB_TYPE_USER;
        System.arraycopy(name, 0, nameKey, 1, name.length);
        return mNodeStore.openOrderedView(nameKey);
    }

    /**
     * Returns a full view into the given named sub database, creating it if
     * necessary. Name is UTF-8 encoded.
     */
    public OrderedView openOrderedView(String name) throws IOException {
        return openOrderedView(name.getBytes("UTF-8"));
    }

    /**
     * Durably commit all changes to the database, while still allowing
     * concurrent access. Commit can be called by any thread, although only one
     * is permitted in at a time.
     */
    public void commit() throws IOException {
        mNodeStore.commit();
    }

    @Override
    public void close() throws IOException {
        mNodeStore.close();
    }
}
