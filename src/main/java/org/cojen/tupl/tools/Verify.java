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

import org.cojen.tupl.*;

/**
 * Simple database verification utility. Main method accepts a single argument &mdash; a base
 * file path for the database. Main method exits with a status of 1 if verification failed, 0
 * if succeeded.
 *
 * @author Brian S O'Neill
 * @see Database#verify Database.verify
 */
public class Verify extends VerificationObserver {
    /**
     * @param args only argument is a base file path for the database
     */
    public static void main(String[] args) throws Exception {
        Database db = Database.open(new DatabaseConfig().baseFilePath(args[0]));
        System.out.println(db.stats());
        Verify v = new Verify();
        db.verify(v);
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
        System.out.println("Index: " + ix.getNameString() + ", height: " + height);
        return super.indexBegin(ix, height);
    }

    @Override
    public boolean indexNodePassed(long id,
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
    public boolean indexNodeFailed(long id, int level, String message) {
        failed = 1;
        return super.indexNodeFailed(id, level, message);
    }

    @Override
    public String toString() {
        // Note: Entry count also includes internal nodes, and so it will exceed the total
        // number of actual keys.
        return "totalNodeCount: " + totalNodeCount +
            ", totalEntryCount: " + totalEntryCount +
            ", totalFreeBytes: " + totalFreeBytes +
            ", totalLargeValues: " + totalLargeValues;
    }
}
