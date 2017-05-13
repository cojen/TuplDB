/*
 *  Copyright (C) 2011-2017 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl;

import java.io.IOException;

import java.util.Arrays;

/**
 * Tree which uses an explicit transaction when none is specified, excluding loads.
 *
 * @author Brian S O'Neill
 */
/*P*/
final class TxnTree extends Tree {
    TxnTree(LocalDatabase db, long id, byte[] idBytes, Node root) {
        super(db, id, idBytes, root);
    }

    @Override
    public TreeCursor newCursor(Transaction txn) {
        return new TxnTreeCursor(this, txn);
    }

    @Override
    public void store(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (txn != null) {
            super.store(txn, key, value);
        } else {
            txnStore(key, value);
        }
    }

    private void txnStore(byte[] key, byte[] value) throws IOException {
        Transaction txn = mDatabase.newAlwaysRedoTransaction();
        try {
            TreeCursor c = new TxnTreeCursor(this, txn);
            try {
                c.mKeyOnly = true;
                c.doFind(key);
                c.commit(value);
            } finally {
                c.reset();
            }
        } catch (Throwable e) {
            txn.reset();
            throw e;
        }
    }

    @Override
    public byte[] exchange(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (txn != null) {
            return super.exchange(txn, key, value);
        } else {
            return txnExchange(key, value);
        }
    }

    private byte[] txnExchange(byte[] key, byte[] value) throws IOException {
        Transaction txn = mDatabase.newAlwaysRedoTransaction();
        try {
            TreeCursor c = new TxnTreeCursor(this, txn);
            try {
                c.doFind(key);
                byte[] oldValue = c.mValue;
                c.commit(value);
                return oldValue;
            } finally {
                c.reset();
            }
        } catch (Throwable e) {
            txn.reset();
            throw e;
        }
    }

    @Override
    public boolean insert(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (txn != null) {
            return super.insert(txn, key, value);
        } else {
            return txnInsert(key, value);
        }
    }

    private boolean txnInsert(byte[] key, byte[] value) throws IOException {
        Transaction txn = mDatabase.newAlwaysRedoTransaction();
        try {
            TreeCursor c = new TxnTreeCursor(this, txn);
            try {
                c.mKeyOnly = true;
                c.doFind(key);
                if (c.mValue == null) {
                    c.commit(value);
                    return true;
                } else {
                    txn.reset();
                    return false;
                }
            } finally {
                c.reset();
            }
        } catch (Throwable e) {
            txn.reset();
            throw e;
        }
    }

    @Override
    public boolean replace(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (txn != null) {
            return super.replace(txn, key, value);
        } else {
            return txnReplace(key, value);
        }
    }

    private boolean txnReplace(byte[] key, byte[] value) throws IOException {
        Transaction txn = mDatabase.newAlwaysRedoTransaction();
        try {
            TreeCursor c = new TxnTreeCursor(this, txn);
            try {
                c.mKeyOnly = true;
                c.doFind(key);
                if (c.mValue != null) {
                    c.commit(value);
                    return true;
                } else {
                    txn.reset();
                    return false;
                }
            } finally {
                c.reset();
            }
        } catch (Throwable e) {
            txn.reset();
            throw e;
        }
    }

    @Override
    public boolean update(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        if (txn != null) {
            return super.update(txn, key, oldValue, newValue);
        } else {
            return txnUpdate(key, oldValue, newValue);
        }
    }

    private boolean txnUpdate(byte[] key, byte[] oldValue, byte[] newValue) throws IOException {
        Transaction txn = mDatabase.newAlwaysRedoTransaction();
        try {
            TreeCursor c = new TxnTreeCursor(this, txn);
            try {
                c.doFind(key);
                if (Arrays.equals(oldValue, c.mValue)) {
                    c.commit(newValue);
                    return true;
                } else {
                    txn.reset();
                    return false;
                }
            } finally {
                c.reset();
            }
        } catch (Throwable e) {
            txn.reset();
            throw e;
        }
    }

    /*
    @Override
    public Stream newStream() {
        TreeCursor cursor = new TxnTreeCursor(this);
        cursor.autoload(false);
        return new TreeValueStream(cursor);
    }
    */
}
