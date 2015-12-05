/*
 *  Copyright 2014-2015 Cojen.org
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
