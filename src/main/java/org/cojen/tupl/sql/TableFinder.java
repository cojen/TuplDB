/*
 *  Copyright (C) 2024 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.sql;

import java.io.IOException;

import org.cojen.tupl.Database;
import org.cojen.tupl.Table;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
@FunctionalInterface
public interface TableFinder {
    /**
     * @return null if not found
     */
    Table findTable(String name) throws IOException;

    /**
     * @param schema base package to use for finding classes; pass null to require fully
     * qualified names
     */
    public static TableFinder using(Database db, String schema) {
        return using(db, schema, null);
    }

    /**
     * @param schema base package to use for finding classes; pass null to require fully
     * qualified names
     */
    public static TableFinder using(Database db, String schema, ClassLoader loader) {
        return name -> db.findTable(findClass(schema, loader, name));
    }

    private static Class<?> findClass(String schema, ClassLoader loader, String name) {
        if (loader == null) {
            loader = ClassLoader.getSystemClassLoader();
        }

        try {
            if (schema == null) {
                return loader.loadClass(name);
            }

            if (name.indexOf('.') < 0) {
                try {
                    // Assume the schema is required.
                    return loader.loadClass(schema + '.' + name);
                } catch (ClassNotFoundException e) {
                    // Might be fully qualified (in the top-level package).
                    return loader.loadClass(name);
                }
            } else {
                try {
                    // Assume the name is already fully qualified.
                    return loader.loadClass(name);
                } catch (ClassNotFoundException e) {
                    return loader.loadClass(schema + '.' + name);
                }
            }
        } catch (ClassNotFoundException e) {
            return null;
        }
    }
}
