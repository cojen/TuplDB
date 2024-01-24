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

package org.cojen.tupl.jdbc;

import java.sql.SQLException;

import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public abstract class DbStatement extends BasePreparedStatement {
    public static interface Factory {
        /**
         * Returns a new DbStatement instance against the given connection.
         */
        DbStatement newDbStatement(DbConnection con);
    }

    private DbConnection mCon;

    // Accessed by DbConnection.
    DbStatement mPrev, mNext;

    protected DbStatement(DbConnection con) {
        super();
        mCon = con;
        con.register(this);
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
        DbConnection con = mCon;
        doClose();
        if (con != null) {
            con.unregister(this);
        }
    }

    final void doClose() throws SQLException {
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
     * Returns an optional QueryPlan for this query.
     *
     * @throws SQLException if not all parameters have been set
     */
    public QueryPlan plan() throws SQLException {
        checkParams();
        return null;
    }

    /**
     * @throws SQLException if not all parameters have been set
     */
    public void checkParams() throws SQLException {
    }

    /**
     * Close and discard the ResultSet, if any.
     */
    protected abstract void closeResultSet() throws SQLException;
}
