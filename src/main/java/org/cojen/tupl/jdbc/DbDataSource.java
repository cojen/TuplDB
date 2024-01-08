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
import java.io.PrintWriter;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import java.util.Objects;

import java.util.logging.Logger;

import javax.sql.DataSource;

import net.sf.jsqlparser.parser.ParseException;

import org.cojen.tupl.Database;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.model.RelationNode;

import org.cojen.tupl.rows.SoftCache;

import org.cojen.tupl.sql.StatementProcessor;
import org.cojen.tupl.sql.TableFinder;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class DbDataSource implements DataSource {
    final Database mDb;
    final TableFinder mFinder;

    private final SoftCache<String, DbQuery.Factory, TableFinder> mStatementCache;

    public DbDataSource(Database db) {
        this(db, null, null);
    }

    /**
     * @param schema base package to use for finding classes; pass null to require fully
     * qualified names by default
     */
    public DbDataSource(Database db, String schema) {
        this(db, schema, null);
    }

    /**
     * @param schema base package to use for finding classes; pass null to require fully
     * qualified names by default
     * @param loader used to load table classes
     */
    public DbDataSource(Database db, String schema, ClassLoader loader) {
        this(db, TableFinder.using(db, schema, loader));
    }

    public DbDataSource(Database db, TableFinder finder) {
        mDb = Objects.requireNonNull(db);
        mFinder = Objects.requireNonNull(finder);

        mStatementCache = new SoftCache<>() {
            @Override
            protected DbQuery.Factory newValue(String sql, TableFinder finder) {
                try {
                    Object stmt;
                    try {
                        stmt = StatementProcessor.process(sql, finder);
                    } catch (ParseException e) {
                        throw new SQLException(e.getMessage());
                    } catch (IOException e) {
                        throw new SQLException(e);
                    }

                    if (stmt instanceof RelationNode rn) {
                        return DbQueryMaker.make(rn.makeTableProvider());
                    }

                    throw new SQLException("Unsupported statement: " + sql + ", " + stmt);
                } catch (SQLException e) {
                    throw Utils.rethrow(e);
                }
            }
        };
    }

    @Override
    public DbConnection getConnection() throws SQLException {
        return new DbConnection(this);
    }

    @Override
    public DbConnection getConnection(String username, String password) throws SQLException {
        return getConnection();
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    DbQuery.Factory queryFactory(String sql, TableFinder finder) throws SQLException {
        return mStatementCache.obtain(sql, finder == null ? mFinder : finder);
    }
}
