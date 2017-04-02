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
