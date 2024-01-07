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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.cojen.tupl.Transaction;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public abstract class DbQuery extends BasePreparedStatement {
    private DbConnection mCon;

    protected DbQuery(DbConnection con) {
        super();
        mCon = con;
    }

    public static interface Factory {
        /**
         * Returns a new DbQuery instance against the given connection.
         */
        DbQuery newDbQuery(DbConnection con);
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

    @Override
    public final DbConnection getConnection() throws SQLException {
        DbConnection con = mCon;
        if (con == null) {
            throw new SQLException("Closed");
        }
        return con;
    }

    @Override
    public final void close() throws SQLException {
        mCon = null;
        closeResultSet();
    }

    @Override
    public final boolean isClosed() throws SQLException {
        return mCon == null;
    }

    protected final void checkClosed() throws SQLException {
        if (mCon == null) {
            throw new SQLException("Closed");
        }
    }

    protected final Transaction txn() throws SQLException {
        return getConnection().txn();
    }

    /**
     * Should be overridden by subclasses which have parameters.
     */
    @Override
    public void clearParameters() throws SQLException {
    }

    /**
     * Should be overridden by subclasses which have parameters.
     */
    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        throw new SQLException("Unknown parameter: " + parameterIndex);
    }

    protected static void setObject(Object[] args, int parameterIndex, Object x)
        throws SQLException
    {
        if (parameterIndex <= 0 || parameterIndex > args.length) {
            throw new SQLException("Unknown parameter: " + parameterIndex);
        }
        args[parameterIndex - 1] = x;
    }

    protected static void checkParams(int expect, int actual) throws SQLException {
        if (expect != actual) {
            throw new SQLException("Not all parameters have been set");
        }
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

    /**
     * Close and discard the ResultSet, if any.
     */
    protected abstract void closeResultSet() throws SQLException;
}
