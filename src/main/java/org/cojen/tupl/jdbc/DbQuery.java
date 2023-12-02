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

    @Override
    public ResultSet executeQuery() throws SQLException {
        return executeQuery(mCon.txn());
    }

    @Override
    public boolean execute() throws SQLException {
        executeQuery();
        return true;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return -1;
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLException("Statement is a query");
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        closeResultSet();
        return false;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return getResultSet().getMetaData();
    }

    @Override
    public DbConnection getConnection() throws SQLException {
        DbConnection con = mCon;
        if (con == null) {
            throw new SQLException("Closed");
        }
        return con;
    }

    @Override
    public void close() throws SQLException {
        mCon = null;
        closeResultSet();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return mCon == null;
    }

    protected void checkClosed() throws SQLException {
        if (mCon == null) {
            throw new SQLException("Closed");
        }
    }

    /* Implemented by subclass.
    @Override
    public void clearParameters() throws SQLException {

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
    */

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
    protected abstract ResultSet executeQuery(Transaction txn) throws SQLException;

    /**
     * Close and clear the ResultSet, if any.
     */
    protected abstract void closeResultSet() throws SQLException;
}
