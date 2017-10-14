/*
 *  Copyright (C) 2017 Cojen.org
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

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class SortTest {
    // FIXME: testing
    public static void main(String[] args) throws Exception {
        DatabaseConfig config = new DatabaseConfig()
            .checkpointSizeThreshold(0)
            .checkpointRate(10, TimeUnit.SECONDS)
            .minCacheSize(10_000_000_000L)
            .durabilityMode(DurabilityMode.NO_FLUSH);

        if (args.length > 0) {
            config.baseFilePath(args[0]);
        }

        Database db = Database.open(config);

        long seed = new Random().nextLong();
        System.out.println("seed: " + seed);
        Random rnd = new Random(seed);

        byte[] key = new byte[8];
        byte[] value = new byte[0];

        final long count = 10_000_000_000L;

        long start = System.currentTimeMillis();

        Sorter s = db.newSorter(null);

        for (long i = 0; i < count; i++) {
            if (i % 1_000_000 == 0) {
                System.out.println(i);
            }
            rnd.nextBytes(key);
            s.add(key, value);
        }

        System.out.println("finish...");
        s.finish();

        long end = System.currentTimeMillis();
        System.out.println("duration: " + (end - start));
    }
}
