/*
 *  Copyright 2020 Cojen.org
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

package org.cojen.tupl.core;

import java.util.zip.CRC32C;

import org.cojen.tupl.DatabaseConfig;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class SnapshotChecksumTest extends SnapshotTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(SnapshotChecksumTest.class.getName());
    }

    @Override
    public void decorate(DatabaseConfig config) throws Exception {
        config.cacheSize(400_000_000L).checksumPages(CRC32C::new);
    }
}
