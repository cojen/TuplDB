/*
 *  Copyright 2014-2015 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl;

import java.io.IOException;

/**
 * TreeCursor which uses an explicit transaction when none is specified, excluding loads.
 *
 * @author Brian S O'Neill
 */
/*P*/
final class TxnTreeCursor extends TreeCursor {
    TxnTreeCursor(TxnTree tree, Transaction txn) {
        super(tree, txn);
    }

    TxnTreeCursor(TxnTree tree) {
        super(tree);
    }

    @Override
    public final void store(byte[] value) throws IOException {
        byte[] key = mKey;
        ViewUtils.positionCheck(key);

        try {
            LocalTransaction txn = mTxn;
            if (txn == null) {
                txn = mTree.mDatabase.newAlwaysRedoTransaction();
                try {
                    if (txn.lockMode() != LockMode.UNSAFE) {
                        txn.lockExclusive(mTree.mId, key, keyHash());
                    }
                    store(txn, leafExclusive(), value);
                    txn.commit();
                } catch (Throwable e) {
                    txn.reset();
                    throw e;
                }
            } else {
                if (txn.lockMode() != LockMode.UNSAFE) {
                    txn.lockExclusive(mTree.mId, key, keyHash());
                }
                store(txn, leafExclusive(), value);
            }
        } catch (Throwable e) {
            throw handleException(e, false);
        }
    }

    @Override
    public final void commit(byte[] value) throws IOException {
        // Cannot use optimizations here. Replicated transactions need rollback support for all
        // operations, including the last one.
        // TODO: Figure out how to still use the combo store-commit redo operations.
        ViewUtils.commit(this, value);
    }
}
