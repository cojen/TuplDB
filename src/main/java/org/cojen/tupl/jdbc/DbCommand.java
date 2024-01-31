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

import java.io.IOException;

import java.util.Arrays;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.model.Command;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class DbCommand extends DbStatement {
    public static DbCommand newDbCommand(DbConnection con, Command command) {
        if (command.argumentCount() == 0) {
            return new DbCommand(con, command);
        } else {
            return new WithParams(con, command);
        }
    }

    protected final Command mCommand;

    protected long mUpdateCount = -1;

    DbCommand(DbConnection con, Command command) {
        super(con);
        mCommand = command;
    }

    @Override
    public final ResultSet getResultSet() throws SQLException {
        return null;
    }

    @Override
    public final boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public final ResultSet executeQuery() throws SQLException {
        throw new SQLException("Statement is not a query");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLException("Statement is not query");
    }

    @Override
    public int executeUpdate() throws SQLException {
        execute();
        return getUpdateCount();
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        execute();
        return getLargeUpdateCount();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return (int) Math.min(getLargeUpdateCount(), Integer.MAX_VALUE);
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        return mUpdateCount;
    }

    @Override
    public boolean execute() throws SQLException {
        try {
            mUpdateCount = mCommand.exec(getConnection().txn());
        } catch (IOException e) {
            throw new SQLException(e);
        }
        return false;
    }

    /* Note that DbStatement provides defaults for commands which have no parameters.
    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {

    @Override
    public void clearParameters() throws SQLException {
    */

    @Override
    public QueryPlan plan() throws SQLException {
        try {
            return mCommand.plan(getConnection().txn());
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    protected final void reset() {
        mUpdateCount = -1;
    }

    private static class WithParams extends DbCommand {
        private final Object[] mParams;

        WithParams(DbConnection con, Command command) {
            super(con, command);
            mParams = new Object[command.argumentCount()];
        }

        @Override
        public boolean execute() throws SQLException {
            checkParams();
            try {
                mUpdateCount = mCommand.exec(getConnection().txn(), mParams);
            } catch (IOException e) {
                throw new SQLException(e);
            }
            return false;
        }

        @Override
        public void setObject(int parameterIndex, Object x) throws SQLException {
            // FIXME: DbQueryMaker also defines state bits, and the execute method calls
            // checkParams.
            setObject(mParams, parameterIndex, x);
        }

        @Override
        public void clearParameters() throws SQLException {
            // FIXME: DbQueryMaker also clears the state bits.
            Arrays.fill(mParams, null);
        }

        @Override
        public QueryPlan plan() throws SQLException {
            checkParams();
            try {
                return mCommand.plan(getConnection().txn(), mParams);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }

        @Override
        public void checkParams() throws SQLException {
            // FIXME
        }
    }
}
