/*
 *  Copyright 2019 Cojen.org
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
 * Opens a database and then calls {@link Database#shutdown shutdown}.
 *
 * @author Brian S O'Neill
 */
public class Shutdown {
    /**
     * @param args a base file path for the database, and an optional cache size
     */
    public static void main(String[] args) throws Exception {
        var config = new DatabaseConfig()
            .createFilePath(false)
            .baseFilePath(args[0])
            .eventListener(new EventPrinter());

        if (args.length > 1) {
            config.minCacheSize(Long.parseLong(args[1]));
        }

        Database db = Database.open(config);
        Database.Stats stats = db.stats();
        db.shutdown();

        System.out.println(stats);
    }

    private Shutdown() {
    }
}
