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

import org.cojen.tupl.core.CoreDatabase;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.model.Command;
import org.cojen.tupl.model.CommandNode;
import org.cojen.tupl.model.Node;
import org.cojen.tupl.model.RelationNode;

import org.cojen.tupl.table.SchemaChangeListener;
import org.cojen.tupl.table.SoftCache;

import org.cojen.tupl.sql.Scope;
import org.cojen.tupl.sql.StatementProcessor;
import org.cojen.tupl.sql.TableFinder;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class DbDataSource implements DataSource {
    final Database mDb;

    private final TableFinder mFinder;

    // Maps schema strings to DbDataSource instances. Is the same cache instance for this
    // DbDataSource and all DbDataSource instances derived from this one.
    private final SoftCache<String, DbDataSource, Object> mWithSchemaCache;

    // Maps sql strings to DbStatement.Factory instances. Each DbDataSource has a distinct
    // cache instance, and so these factories are tied to a specific schema.
    private final SoftCache<String, DbStatement.Factory, Object> mStatementCache;

    public DbDataSource(Database db) throws SQLException {
        this(db, "", null);
    }

    /**
     * @param schema base package to use for finding classes; pass an empty string to require
     * fully qualified names by default
     */
    public DbDataSource(Database db, String schema) throws SQLException {
        this(db, schema, null);
    }

    /**
     * @param schema base package to use for finding classes; pass an empty string to require
     * fully qualified names by default
     * @param loader used to load table classes
     */
    public DbDataSource(Database db, String schema, ClassLoader loader) throws SQLException {
        this(db, newTableFinder(db, schema, loader));
    }

    private static TableFinder newTableFinder(Database db, String schema, ClassLoader loader)
        throws SQLException
    {
        try {
            return TableFinder.using(db, schema, loader);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    public DbDataSource(Database db, TableFinder finder) throws SQLException {
        this(null, Objects.requireNonNull(db), Objects.requireNonNull(finder));
        // This is the "root" DbDataSource, which must be cached explicitly.
        mWithSchemaCache.put(finder.schema(), this);
    }

    private DbDataSource(SoftCache<String, DbDataSource, Object> withSchemaCache,
                         Database db, TableFinder finder)
        throws SQLException
    {
        mDb = db;
        mFinder = finder;

        if (withSchemaCache == null) {
            withSchemaCache = new SoftCache<>() {
                @Override
                protected DbDataSource newValue(String schema, Object unused) {
                    try {
                        return new DbDataSource(this, mDb, mFinder.withSchema(schema));
                    } catch (SQLException e) {
                        throw Utils.rethrow(e);
                    }
                }
            };

            try {
                ((CoreDatabase) db).rowStore().addSchemaChangeListener(this::schemaChanged);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }

        mWithSchemaCache = withSchemaCache;

        mStatementCache = new SoftCache<>() {
            @Override
            protected DbStatement.Factory newValue(String sql, Object unused) {
                try {
                    return newStatementFactory(sql);
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

    String schema() {
        return mFinder.schema();
    }

    DbDataSource withSchema(String schema) {
        return mFinder.schema().equals(schema) ? this : mWithSchemaCache.obtain(schema, null);
    }

    DbStatement.Factory statementFactory(String sql) throws SQLException {
        return mStatementCache.obtain(sql, null);
    }

    private void schemaChanged(long tableId, int changes) {
        if ((changes & ~SchemaChangeListener.CHANGE_CREATE) == 0) {
            // Don't need to clear the statement cache when only a table is added.
            return;
        }

        /*
          Clearing the cache allows newly requested statements to observe any changes to the
          table, but statement instances which are directly referenced by the application
          aren't affected. Is this a feature or a flaw? I'm not sure. Statement instances
          aren't usually referenced indefinitely because they're bound to a connection.

          Here's a problem: If the statement is an "insert", and the cache is flushed because a
          column was added, the old insert statement will insert an empty value for the new
          column instead of failing.
        */

        mWithSchemaCache.traverse((DbDataSource ds) -> ds.mStatementCache.clear());
    }

    private DbStatement.Factory newStatementFactory(String sql) throws SQLException {
        Node stmt;
        try {
            stmt = StatementProcessor.process(sql, new Scope(mFinder));
        } catch (IOException e) {
            throw new SQLException(e);
        } catch (NullPointerException e) {
            throw new SQLException(e);
        } catch (Exception e) {
            if (e instanceof SQLException se) {
                throw se;
            }
            String message = e.getMessage();
            if (message != null && !message.isEmpty()) {
                throw new SQLException(message);
            } else {
                throw new SQLException(e);
            }
        }

        if (stmt instanceof RelationNode rn) {
            return DbQueryMaker.make(rn.makeTableProvider(), schema());
        }

        if (stmt instanceof CommandNode cn) {
            Command command = cn.makeCommand();
            return (DbConnection con) -> DbCommand.newDbCommand(con, command);
        }

        throw new SQLException("Unsupported statement: " + sql + ", " + stmt);
    }
}
