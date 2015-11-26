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

import java.io.File;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ConfigTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ConfigTest.class.getName());
    }

    @Test
    public void pageSize() throws Exception {
        Database.open(new DatabaseConfig().pageSize(512));
        Database.open(new DatabaseConfig().pageSize(65536));
        Database.open(new DatabaseConfig().pageSize(0));
        Database.open(new DatabaseConfig().pageSize(-1));

        try {
            Database.open(new DatabaseConfig().pageSize(511));
            fail();
        } catch (IllegalArgumentException e) {
        }

        try {
            Database.open(new DatabaseConfig().pageSize(65537));
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void cacheSize() throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.minCacheSize(1000);
        config.maxCacheSize(999);
        try {
            Database.open(config);
            fail();
        } catch (IllegalArgumentException e) {
        }

        // Defaults to minimum allowed.
        config.minCacheSize(-10);
        config.maxCacheSize(-8);
        Database.open(config);

        config.minCacheSize(1000);
        config.maxCacheSize(-1);
        Database.open(config);

        try {
            config.minCacheSize(Long.MAX_VALUE);
            Database.open(config);
            fail();
        } catch (OutOfMemoryError e) {
        }
    }

    @Test
    public void files() throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.dataFile(new File("foo"));
        try {
            Database.open(config);
            fail();
        } catch (IllegalArgumentException e) {
            // Must have a base file.
        }

        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        config.baseFile(tempDir);

        try {
            Database.open(config);
            fail();
        } catch (IllegalArgumentException e) {
            // Base file cannot be a directory.
        }

        config.baseFile(new File(tempDir, "test"));
        config.dataFile(tempDir);

        try {
            Database.open(config);
            fail();
        } catch (IllegalArgumentException e) {
            // Data file cannot be a directory.
        }
    }
}
