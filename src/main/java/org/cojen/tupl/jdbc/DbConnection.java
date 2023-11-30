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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;

import org.cojen.tupl.Database;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.Transaction;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class DbConnection extends BaseConnection {
    private Database mDb;
    private Transaction mTxn;
    private TxnSavepoint mLastSavepoint;

    public DbConnection(Database db) {
        mDb = db;
    }

    @Override
    public Statement createStatement() throws SQLException {
        // FIXME
        throw null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        // FIXME
        throw null;
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
        mDb = null;
        mTxn = null;
        // FIXME: Close all Statements and ResultSets too.
    }

    @Override
    public boolean isClosed() throws SQLException {
        return mDb == null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        Transaction txn;
        LockMode mode;

        obtainTxn: {
            switch (level) {
            case TRANSACTION_SERIALIZABLE:
                txn = mTxn;
                if (txn != null) {
                    mode = LockMode.UPGRADABLE_READ;
                    break obtainTxn;
                }
                mTxn = db().newTransaction();
                return;

            case TRANSACTION_REPEATABLE_READ:
                mode = LockMode.READ_COMMITTED;
                break;
 
            case TRANSACTION_READ_COMMITTED:
                txn = mTxn;
                if (txn != null) {
                    mode = LockMode.READ_COMMITTED;
                    break obtainTxn;
                }
                return;

            case TRANSACTION_READ_UNCOMMITTED:
                mode = LockMode.READ_UNCOMMITTED;
                break;

            default: throw new SQLException("Unknown level");
            }

            txn = mTxn;

            if (txn == null) {
                mTxn = txn = db().newTransaction();
            }
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

    private Database db() throws SQLException {
        Database db = mDb;
        if (db == null) {
            throw new SQLException("Closed");
        }
        return db;
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
