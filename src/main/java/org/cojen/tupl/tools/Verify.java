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

package org.cojen.tupl.tools;

import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.Index;

import org.cojen.tupl.diag.EventListener;
import org.cojen.tupl.diag.VerificationObserver;

/**
 * Simple database verification utility. The main method requires a single argument &mdash; a
 * base file path for the database. An optional cache size can be provided too. The main method
 * exits with a status of 1 if verification failed, or else 0 if it succeeded.
 *
 * @author Brian S O'Neill
 * @see Database#verify Database.verify
 */
public class Verify extends VerificationObserver {
    /**
     * @param args first argument is a base file path for the database, second optional
     * argument is the cache size
     */
    public static void main(String[] args) throws Exception {
        var config = new DatabaseConfig()
            .createFilePath(false)
            .baseFilePath(args[0])
            .eventListener(EventListener.printTo(System.out));

        if (args.length > 1) {
            config.minCacheSize(Long.parseLong(args[1]));
        }
        
        Database db = Database.open(config);

        System.out.println(db.stats());

        var v = new Verify();
        db.verify(v, 0);
        System.out.println(v);
        System.exit(v.failed);
    }

    private int failed;
    private long totalNodeCount;
    private long totalEntryCount;
    private long totalFreeBytes;
    private long totalLargeValues;

    @Override
    public boolean indexBegin(Index ix, int height) {
        String nameStr = ix.nameString();
        if (nameStr == null) {
            nameStr = String.valueOf(ix.id());
        }
        System.out.println("Index: " + nameStr + ", height: " + height);
        return super.indexBegin(ix, height);
    }

    @Override
    public synchronized boolean indexNodePassed(long id,
                                                int level,
                                                int entryCount,
                                                int freeBytes,
                                                int largeValueCount)
    {
        totalEntryCount += entryCount;
        totalFreeBytes += freeBytes;
        totalLargeValues += largeValueCount;
        if (((++totalNodeCount) % 10000) == 0) {
            System.out.println(this);
        }
        return true;
    }

    @Override
    public synchronized boolean indexNodeFailed(long id, int level, String message) {
        failed = 1;
        return super.indexNodeFailed(id, level, message);
    }

    @Override
    public String toString() {
        // Note: Entry count also includes internal nodes, and so it will exceed the total
        // number of actual keys.
        return "nodeCount: " + totalNodeCount +
            ", entryCount: " + totalEntryCount +
            ", freeBytes: " + totalFreeBytes +
            ", largeValues: " + totalLargeValues;
    }
}
