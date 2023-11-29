/*
 *  Copyright (C) 2023 Cojen.org
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

package org.cojen.tupl.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * Implements as many sensible ResultSetMetaData defaults as possible. Also defines a
 * getColumnClass method, which isn't defined in ResultSetMetaData.
 *
 * @author Brian S O'Neill
 */
public abstract class BaseResultSetMetaData implements ResultSetMetaData {
    /* Implemented by ResultSetMaker.
    @Override
    public int getColumnCount() throws SQLException {

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
    */

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    /* Implemented by ResultSetMaker.
    @Override
    public int isNullable(int column) throws SQLException {

    @Override
    public boolean isSigned(int column) throws SQLException {

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
    */

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    /* Implemented by ResultSetMaker.
    @Override
    public String getColumnName(int column) throws SQLException {
    */

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    /* Implemented by ResultSetMaker.
    @Override
    public int getPrecision(int column) throws SQLException {

    @Override
    public int getScale(int column) throws SQLException {
    */

    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    /* Implemented by ResultSetMaker.
    @Override
    public int getColumnType(int column) throws SQLException {
    */

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return getColumnClass(column).getSimpleName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return true;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return getColumnClass(column).getCanonicalName();
    }

    public abstract Class<?> getColumnClass(int column) throws SQLException;

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
