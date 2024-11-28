/*
 *  Copyright (C) 2011-2018 Cojen.org
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

package org.cojen.tupl.core;

import java.io.IOException;

import org.cojen.tupl.LockMode;

/**
 * Used to apply recovered transactions from the redo log, when database isn't replicated.
 * This class extends ReplEngine because it applies transactions using multiple threads,
 * but its replication features aren't used.
 *
 * @author Brian S O'Neill
 */
final class RedoLogApplier extends ReplEngine {
    private long mHighestTxnId;

    /**
     * @param maxThreads pass zero to use all processors; see DatabaseConfig.maxReplicaThreads
     */
    RedoLogApplier(int maxThreads, LocalDatabase db, LHashTable.Obj<LocalTransaction> txns,
                   LHashTable.Obj<BTreeCursor> cursors)
        throws IOException
    {
        super(null, maxThreads, db, txns, cursors);
    }

    /**
     * Return the highest observed transaction id.
     *
     * @param txnId transaction id recovered from the database header
     */
    public long highestTxnId(long txnId) {
        if (mHighestTxnId != 0) {
            // Subtract for modulo comparison.
            if (txnId == 0 || (mHighestTxnId - txnId) > 0) {
                txnId = mHighestTxnId;
            }
        }
        return txnId;
    }

    @Override
    public Thread newThread(Runnable r) {
        var t = new Thread(r);
        t.setDaemon(true);
        t.setName("Recovery-" + Long.toUnsignedString(t.threadId()));
        t.setUncaughtExceptionHandler((thread, ex) -> Utils.closeQuietly(mDatabase, ex));
        return t;
    }

    @Override
    public boolean reset() throws IOException {
        // Ignore resets until the very end.
        return true;
    }

    @Override
    protected LocalTransaction newTransaction(long txnId) {
        if (txnId > mHighestTxnId) {
            mHighestTxnId = txnId;
        }

        var txn = new LocalTransaction
            (mDatabase, txnId, LockMode.UPGRADABLE_READ, INFINITE_TIMEOUT);

        txn.attach(attachment());

        return txn;
    }

    @Override
    protected Object attachment() {
        return "recovery";
    }
}
