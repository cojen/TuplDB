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

import java.util.Objects;

import org.cojen.tupl.Database;
import org.cojen.tupl.Table;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public interface TableFinder {
    /**
     * @return null if not found
     */
    Table findTable(String name) throws IOException;

    /**
     * @return optional base package to use for finding classes
     */
    String schema();

    /**
     * @param schema base package to use for finding classes; pass null to require fully
     * qualified names
     */
    TableFinder withSchema(String schema);

    public static TableFinder using(Database db) {
        return using(db, null, null);
    }

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
     * @param loader used to load table classes
     */
    public static TableFinder using(Database db, String schema, ClassLoader loader) {
        return new Basic(db, schema, loader);
    }

    static class Basic implements TableFinder {
        private final Database mDb;
        private final String mSchema;
        private final ClassLoader mLoader;

        private Basic(Database db, String schema, ClassLoader loader) {
            mDb = db;
            mSchema = schema;
            if (loader == null) {
                loader = ClassLoader.getSystemClassLoader();
            }
            mLoader = loader;
        }

        @Override
        public Table findTable(String name) throws IOException {
            Class<?> clazz = findClass(name);
            return clazz == null ? null : mDb.findTable(clazz);
        }

        @Override
        public String schema() {
            return mSchema;
        }

        @Override
        public TableFinder withSchema(String schema) {
            return Objects.equals(mSchema, schema) ? this : new Basic(mDb, schema, mLoader);
        }

        private Class<?> findClass(String name) {
            try {
                if (mSchema == null) {
                    return mLoader.loadClass(name);
                }

                if (name.indexOf('.') < 0) {
                    try {
                        // Assume the schema is required.
                        return mLoader.loadClass(mSchema + '.' + name);
                    } catch (ClassNotFoundException e) {
                        // Might be fully qualified (in the top-level package).
                        return mLoader.loadClass(name);
                    }
                } else {
                    try {
                        // Assume the name is already fully qualified.
                        return mLoader.loadClass(name);
                    } catch (ClassNotFoundException e) {
                        return mLoader.loadClass(mSchema + '.' + name);
                    }
                }
            } catch (ClassNotFoundException e) {
                return null;
            }
        }
    }
}
