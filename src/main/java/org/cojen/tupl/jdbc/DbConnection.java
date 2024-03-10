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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;

import org.cojen.tupl.Database;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class DbConnection extends BaseConnection {
    private static final VarHandle cDataSourceHandle;

    static {
        try {
            cDataSourceHandle = MethodHandles.lookup().findVarHandle
                (DbConnection.class, "mDataSource", DbDataSource.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    private DbDataSource mDataSource;
    private Transaction mTxn;
    private TxnSavepoint mLastSavepoint;

    // Linked list of registered queries.
    private DbStatement mLastStatement;

    DbConnection(DbDataSource dataSource) {
        mDataSource = dataSource;
    }

    @Override
    public Statement createStatement() throws SQLException {
        dataSource(); // check if closed
        return new ConStatement(this);
    }

    @Override
    public DbStatement prepareStatement(String sql) throws SQLException {
        return dataSource().statementFactory(sql).newDbStatement(this);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        Transaction txn = mTxn;
        if (autoCommit) {
            if (txn != null) {
                doCommit(txn);
                mTxn = null;
            }
        } else if (txn == null) {
            // A new transaction is UPGRADABLE_READ, which will be treated as serializable.
            // See BaseTable.newScanner, BaseTableIndex.newScanner, and BaseTable.newUpdater.
            mTxn = db().newTransaction();
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return mTxn == null;
    }

    @Override
    public void commit() throws SQLException {
        Transaction txn = mTxn;
        if (txn != null) {
            doCommit(txn);
        }
    }

    private void doCommit(Transaction txn) throws SQLException {
        try {
            mLastSavepoint = null;
            txn.commitAll();
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void rollback() throws SQLException {
        Transaction txn = mTxn;
        if (txn != null) {
            doRollback(txn);
        }
    }

    private void doRollback(Transaction txn) throws SQLException {
        try {
            mLastSavepoint = null;
            txn.reset();
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        rollback();
        cDataSourceHandle.setRelease(this, null);
        mTxn = null;

        DbStatement last = mLastStatement;
        if (last != null) {
            mLastStatement = null;
            Throwable ex = null;
            while (true) {
                last.mNext = null;
                try {
                    last.doClose();
                } catch (Throwable e) {
                    if (ex == null) {
                        ex = e;
                    } else {
                        Utils.suppress(ex, e);
                    }
                }
                last = last.mPrev;
                if (last == null) {
                    break;
                }
                last.mPrev = null;
            }
            if (ex != null) {
                throw Utils.rethrow(ex);
            }
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return cDataSourceHandle.getAcquire(this) == null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        // Note: See BaseTable.newScanner, BaseTableIndex.newScanner, and BaseTable.newUpdater
        // to see how predicate locks are used in conjunction with transaction lock modes.

        Transaction txn = mTxn;
        LockMode mode;

        switch (level) {
        case TRANSACTION_SERIALIZABLE:
            // Setting the transaction mode to UPGRADABLE_READ is sufficient, because a
            // predicate lock will also be used to ensure serializability.
            if (txn == null) {
                // New transactions are UPGRADABLE_READ, so no need to explicitly set the mode.
                mTxn = db().newTransaction();
            } else {
                txn.lockMode(LockMode.UPGRADABLE_READ);
            }
            return;

        case TRANSACTION_REPEATABLE_READ:
            // When scanning a secondary index, this mode will also use a predicate lock, and
            // so it's effectively serializable too. The difference is that read locks are used
            // (instead of upgradable locks), and so attempting to modify rows acquired with
            // this mode will fail. See IllegalUpgradeException and LockUpgradeRule.
            mode = LockMode.REPEATABLE_READ;
            break;
 
        case TRANSACTION_READ_COMMITTED:
            // Note that a null transaction also behaves like READ_COMMITTED, except that a
            // predicate lock will never be used, and so it's weaker than a transaction which
            // explicitly uses READ_COMMITTED. When scanning rows with a null transaction over
            // a secondary index, rows which were already seen could move ahead and thus be
            // seen again. The use of a predicate lock prevents this. If no predicate lock is
            // desired, then setAutoCommit(true) should be used instead, which sets the
            // transaction to null.
            mode = LockMode.READ_COMMITTED;
            break;

        case TRANSACTION_READ_UNCOMMITTED:
            mode = LockMode.READ_UNCOMMITTED;
            break;

        default: throw new SQLException("Unknown level");
        }

        if (txn == null) {
            mTxn = txn = db().newTransaction();
        }

        txn.lockMode(mode);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        Transaction txn = mTxn;
        if (txn == null) {
            return TRANSACTION_READ_COMMITTED;
        }
        return switch (txn.lockMode()) {
            case UPGRADABLE_READ -> TRANSACTION_SERIALIZABLE;
            case REPEATABLE_READ -> TRANSACTION_REPEATABLE_READ;
            case READ_COMMITTED -> TRANSACTION_READ_COMMITTED;
            case READ_UNCOMMITTED -> TRANSACTION_READ_UNCOMMITTED;
            case UNSAFE -> TRANSACTION_NONE;
        };
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return doSetSavepoint(new TxnSavepoint());
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return doSetSavepoint(new NamedSavepoint(name));
    }

    private Savepoint doSetSavepoint(TxnSavepoint savepoint) throws SQLException {
        Transaction txn = mTxn;

        if (txn == null) {
            mTxn = txn = db().newTransaction();
        } else {
            TxnSavepoint last = mLastSavepoint;
            if (last != null) {
                savepoint.mPrev = last;
                try {
                    LockMode mode = txn.lockMode();
                    txn.enter();
                    txn.lockMode(mode);
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }
        }

        mLastSavepoint = savepoint;
        return savepoint;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        Transaction txn = mTxn;

        if (txn != null) {
            TxnSavepoint last = mLastSavepoint;
            int n = 1;

            while (last != null) {
                TxnSavepoint prev = last.mPrev;

                if (savepoint == last) {
                    mLastSavepoint = prev;

                    do {
                        try {
                            txn.exit();
                        } catch (IOException e) {
                            throw new SQLException(e);
                        }
                    } while (--n > 0);

                    return;
                }

                last = prev;
                n++;
            }
        }

        throw new SQLException("Unknown savepoint");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        TxnSavepoint last = mLastSavepoint;
        TxnSavepoint prev = last.mPrev;

        if (savepoint == last) {
            mLastSavepoint = prev;
            return;
        }

        while (prev != null) {
            if (savepoint == prev) {
                last.mPrev = prev.mPrev;
                return;
            }
            last = prev;
            prev = last.mPrev;
        }

        throw new SQLException("Unknown savepoint");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        if (schema == null) {
            schema = "";
        }

        while (true) {
            DbDataSource dataSource = dataSource();
            DbDataSource newDataSource = dataSource.withSchema(schema);
            if (newDataSource == dataSource) {
                return;
            }
            var oldDataSource = (DbDataSource) cDataSourceHandle
                .compareAndExchange(this, dataSource, newDataSource);
            if (oldDataSource == dataSource) {
                return;
            }
        }
    }

    @Override
    public String getSchema() throws SQLException {
        return dataSource().schema();
    }

    Transaction txn() {
        return mTxn;
    }

    void register(DbStatement statement) {
        DbStatement last = mLastStatement;
        if (last != null) {
            statement.mPrev = last;
            last.mNext = statement;
        }
        mLastStatement = statement;
    }

    void unregister(DbStatement statement) {
        DbStatement prev = statement.mPrev;
        DbStatement next = statement.mNext;
        if (prev != null) {
            prev.mNext = next;
            statement.mPrev = null;
        }
        if (next == null) {
            mLastStatement = prev;
        } else {
            next.mPrev = prev;
            statement.mNext = null;
        }
    }

    private DbDataSource dataSource() throws SQLException {
        var dataSource = (DbDataSource) cDataSourceHandle.getAcquire(this);
        if (dataSource == null) {
            throw new SQLException("Closed");
        }
        return dataSource;
    }

    private Database db() throws SQLException {
        return dataSource().mDb;
    }

    private static class TxnSavepoint implements Savepoint {
        TxnSavepoint mPrev;

        @Override
        public int getSavepointId() throws SQLException {
            return 0;
        }

        @Override
        public String getSavepointName() throws SQLException {
            throw new SQLException();
        }
    }

    private static class NamedSavepoint extends TxnSavepoint {
        private final String mName;

        NamedSavepoint(String name) {
            mName = name;
        }

        @Override
        public String getSavepointName() throws SQLException {
            return mName;
        }
    }
}