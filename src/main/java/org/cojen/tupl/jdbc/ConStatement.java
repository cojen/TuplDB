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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Generic Statement which wraps a Connection and delegates to a PreparedStatement.
 *
 * @author Brian S. O'Neill
 */
final class ConStatement extends BaseStatement {
    private Connection mCon;
    private PreparedStatement mStatement;

    ConStatement(Connection con) {
        mCon = con;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        PreparedStatement stmt = mStatement;
        if (stmt != null) {
            stmt.close();
        }
        stmt = getConnection().prepareStatement(sql);
        boolean result = stmt.execute();
        mStatement = stmt;
        return result;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        PreparedStatement stmt = mStatement;
        if (stmt != null) {
            stmt.close();
        }
        stmt = getConnection().prepareStatement(sql);
        ResultSet result = stmt.executeQuery();
        mStatement = stmt;
        return result;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        PreparedStatement stmt = mStatement;
        if (stmt != null) {
            stmt.close();
        }
        stmt = getConnection().prepareStatement(sql);
        int result = stmt.executeUpdate();
        mStatement = stmt;
        return result;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? null : stmt.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? -1 : stmt.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? false : stmt.getMoreResults();
    }

    @Override
    public Connection getConnection() throws SQLException {
        Connection con = mCon;
        if (con == null) {
            throw new SQLException("Closed");
        }
        return con;
    }

    @Override
    public void close() throws SQLException {
        mCon = null;
        PreparedStatement stmt = mStatement;
        if (stmt != null) {
            stmt.close();
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return mCon == null;
    }
}
