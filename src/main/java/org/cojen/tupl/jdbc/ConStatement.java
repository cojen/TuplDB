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
import java.sql.SQLWarning;

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
    public ResultSet executeQuery(String sql) throws SQLException {
        PreparedStatement stmt = resetStatement().prepareStatement(sql);
        ResultSet result = stmt.executeQuery();
        mStatement = stmt;
        return result;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        PreparedStatement stmt = resetStatement().prepareStatement(sql);
        int result = stmt.executeUpdate();
        mStatement = stmt;
        return result;
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
    public int getMaxFieldSize() throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? super.getMaxFieldSize() : stmt.getMaxFieldSize();
    }

    /*
    @Override
    public void setMaxFieldSize(int max) throws SQLException {
    */

    @Override
    public int getMaxRows() throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? super.getMaxRows() : stmt.getMaxRows();
    }

    /*
    @Override
    public void setMaxRows(int max) throws SQLException {
    */

    @Override
    public long getLargeMaxRows() throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? super.getLargeMaxRows() : stmt.getLargeMaxRows();
    }

    /*
    @Override
    public void setLargeMaxRows(long max) throws SQLException {
    */

    /*
    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
    */

    @Override
    public int getQueryTimeout() throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? super.getQueryTimeout() : stmt.getQueryTimeout();
    }

    /*
    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
    */

    @Override
    public void cancel() throws SQLException {
        PreparedStatement stmt = mStatement;
        if (stmt != null) {
            stmt.cancel();
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? super.getWarnings() : stmt.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        PreparedStatement stmt = mStatement;
        if (stmt != null) {
            stmt.clearWarnings();
        }
    }

    /*
    @Override
    public void setCursorName(String name) throws SQLException {
    */

    @Override
    public boolean execute(String sql) throws SQLException {
        PreparedStatement stmt = resetStatement().prepareStatement(sql);
        boolean result = stmt.execute();
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
    public long getLargeUpdateCount() throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? -1 : stmt.getLargeUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? false : stmt.getMoreResults();
    }

    /*
    @Override
    public void setFetchDirection(int direction) throws SQLException {
    */

    @Override
    public int getFetchDirection() throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? super.getFetchDirection() : stmt.getFetchDirection();
    }

    /*
    @Override
    public void setFetchSize(int rows) throws SQLException {
    */

    @Override
    public int getFetchSize() throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? super.getFetchSize() : stmt.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? super.getResultSetConcurrency() : stmt.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType()  throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? super.getResultSetType() : stmt.getResultSetType();
    }

    /*
    @Override
    public void addBatch(String sql) throws SQLException {

    @Override
    public void clearBatch() throws SQLException {

    @Override
    public int[] executeBatch() throws SQLException {

    @Override
    public long[] executeLargeBatch() throws SQLException {
    */

    @Override
    public Connection getConnection()  throws SQLException {
        Connection con = mCon;
        if (con == null) {
            throw new SQLException("Closed");
        }
        return con;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? false : stmt.getMoreResults();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? super.getGeneratedKeys() : stmt.getGeneratedKeys();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        PreparedStatement stmt = resetStatement().prepareStatement(sql, autoGeneratedKeys);
        int result = stmt.executeUpdate();
        mStatement = stmt;
        return result;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        PreparedStatement stmt = resetStatement().prepareStatement(sql, columnIndexes);
        int result = stmt.executeUpdate();
        mStatement = stmt;
        return result;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        PreparedStatement stmt = resetStatement().prepareStatement(sql, columnNames);
        int result = stmt.executeUpdate();
        mStatement = stmt;
        return result;
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        PreparedStatement stmt = resetStatement().prepareStatement(sql);
        long result = stmt.executeLargeUpdate();
        mStatement = stmt;
        return result;
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        PreparedStatement stmt = resetStatement().prepareStatement(sql, autoGeneratedKeys);
        long result = stmt.executeLargeUpdate();
        mStatement = stmt;
        return result;
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        PreparedStatement stmt = resetStatement().prepareStatement(sql, columnIndexes);
        long result = stmt.executeLargeUpdate();
        mStatement = stmt;
        return result;
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        PreparedStatement stmt = resetStatement().prepareStatement(sql, columnNames);
        long result = stmt.executeLargeUpdate();
        mStatement = stmt;
        return result;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        PreparedStatement stmt = resetStatement().prepareStatement(sql, autoGeneratedKeys);
        boolean result = stmt.execute();
        mStatement = stmt;
        return result;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        PreparedStatement stmt = resetStatement().prepareStatement(sql, columnIndexes);
        boolean result = stmt.execute();
        mStatement = stmt;
        return result;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        PreparedStatement stmt = resetStatement().prepareStatement(sql, columnNames);
        boolean result = stmt.execute();
        mStatement = stmt;
        return result;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? super.getResultSetHoldability() : stmt.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return mCon == null;
    }

    /*
    @Override
    public void setPoolable(boolean poolable) throws SQLException {
    */

    @Override
    public boolean isPoolable() throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? super.isPoolable() : stmt.isPoolable();
    }

    /*
    @Override
    public void closeOnCompletion() throws SQLException {
    */

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? super.isCloseOnCompletion() : stmt.isCloseOnCompletion();
    }

    @Override
    public String enquoteLiteral(String val) throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? super.enquoteLiteral(val) : stmt.enquoteLiteral(val);
    }

    @Override
    public String enquoteIdentifier(String identifier, boolean alwaysQuote) throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? super.enquoteIdentifier(identifier, alwaysQuote)
            : stmt.enquoteIdentifier(identifier, alwaysQuote);
    }

    @Override
    public boolean isSimpleIdentifier(String identifier) throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? super.isSimpleIdentifier(identifier)
            : stmt.isSimpleIdentifier(identifier);
    }

    @Override
    public String enquoteNCharLiteral(String val)  throws SQLException {
        PreparedStatement stmt = mStatement;
        return stmt == null ? super.enquoteNCharLiteral(val) : stmt.enquoteNCharLiteral(val);
    }

    /*
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
    */

    private Connection resetStatement() throws SQLException {
        PreparedStatement stmt = mStatement;
        if (stmt != null) {
            stmt.close();
        }
        return getConnection();
    }
}
