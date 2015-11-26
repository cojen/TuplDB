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
 * Simple database file compaction utility. Main method accepts two arguments &mdash; a base
 * file path for the database and a compaction target.
 *
 * @author Brian S O'Neill
 * @see Database#compactFile Database.compactFile
 */
public class Compact {
    /**
     * @param args a base file path for the database and a compaction target
     */
    public static void main(String[] args) throws Exception {
        Database db = Database.open(new DatabaseConfig()
                                    .baseFilePath(args[0])
                                    .checkpointSizeThreshold(0));

        double target = Double.parseDouble(args[1]);
                                    
        System.out.println("Before: " + db.stats());

        db.compactFile(null, target);

        System.out.println("After: " + db.stats());
    }

    private Compact() {
    }
}
