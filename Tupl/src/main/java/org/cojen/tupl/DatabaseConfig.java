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

import java.io.File;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class DatabaseConfig {
    File mBaseFile;
    int mMinCache;
    int mMaxCache;
    boolean mReadOnly;
    int mPageSize;

    public static DatabaseConfig newConfig() {
        return new DatabaseConfig();
    }

    public DatabaseConfig() {
    }

    public DatabaseConfig setBaseFile(File file) {
        mBaseFile = file;
        return this;
    }

    public DatabaseConfig setMinCachedNodes(int min) {
        mMinCache = min;
        return this;
    }

    public DatabaseConfig setMaxCachedNodes(int max) {
        mMaxCache = max;
        return this;
    }

    public DatabaseConfig setReadOnly(boolean readOnly) {
        mReadOnly = readOnly;
        return this;
    }

    public DatabaseConfig setPageSize(int size) {
        mPageSize = size;
        return this;
    }
}
