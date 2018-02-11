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
 * Simple database file compaction utility. Main method accepts two arguments &mdash; a base
 * file path for the database and a compaction target. An optional cache size can be provided
 * too.
 *
 * @author Brian S O'Neill
 * @see Database#compactFile Database.compactFile
 */
public class Compact {
    /**
     * @param args a base file path for the database, a compaction target, and an optional
     * cache size
     */
    public static void main(String[] args) throws Exception {
        DatabaseConfig config = new DatabaseConfig()
            .baseFilePath(args[0])
            .eventListener(new EventPrinter().ignore(EventType.Category.CHECKPOINT))
            .checkpointSizeThreshold(0);

        double target = Double.parseDouble(args[1]);
                                    
        if (args.length > 2) {
            config.minCacheSize(Long.parseLong(args[2]));
        }

        Database db = Database.open(config);

        System.out.println("Before: " + db.stats());

        db.compactFile(null, target);

        System.out.println("After: " + db.stats());
    }

    private Compact() {
    }
}
