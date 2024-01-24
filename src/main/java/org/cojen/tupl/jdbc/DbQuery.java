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

import java.io.IOException;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.cojen.tupl.Table;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public abstract class DbQuery extends DbStatement {
    protected DbQuery(DbConnection con) {
        super(con);
    }

    @Override
    public final boolean execute() throws SQLException {
        executeQuery();
        return true;
    }

    @Override
    public final int getUpdateCount() throws SQLException {
        return -1;
    }

    @Override
    public final int executeUpdate() throws SQLException {
        throw new SQLException("Statement is a query");
    }

    @Override
    public final boolean getMoreResults() throws SQLException {
        closeResultSet();
        return false;
    }

    @Override
    public final ResultSetMetaData getMetaData() throws SQLException {
        return getResultSet().getMetaData();
    }

    /**
     * Returns a new or existing ResultSet instance. Implementation must call checkClosed
     * before creating a new ResultSet.
     */
    @Override
    public abstract ResultSet getResultSet() throws SQLException;

    /**
     * Returns a new or existing ResultSet instance, initialized to the first row. If the
     * current ResultSet is already initialized, close it first. Implementation must call
     * checkClosed before creating a new ResultSet.
     */
    @Override
    public abstract ResultSet executeQuery() throws SQLException;

    @Override
    public QueryPlan plan() throws SQLException {
        checkParams();

        try {
            Table<?> table = table();
            return table == null ? null : table.queryAll().scannerPlan(txn());
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Returns an optional Table for this query. This method doesn't check if all the
     * parameters have been set, and so attempting to scan it might throw a
     * NullPointerException when converting an unset/null parameter to a primitive type.
     */
    public Table<?> table() {
        return null;
    }
}
