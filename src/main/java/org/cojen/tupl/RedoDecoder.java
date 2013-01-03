/*
 *  Copyright 2012-2013 Brian S O'Neill
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

import static org.cojen.tupl.RedoOps.*;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see RedoWriter
 */
abstract class RedoDecoder {
    private long mTxnId;

    RedoDecoder(long initialTxnId) {
        mTxnId = initialTxnId;
    }

    /**
     * Reads from the given stream, passing operations to the visitor, until
     * the end of stream is reached or visitor returns false.
     */
    void run(DataIn in, RedoVisitor visitor) throws IOException {
        int op;
        while ((op = in.read()) >= 0) {
            switch (op &= 0xff) {
            default:
                throw new DatabaseException("Unknown redo log operation: " + op);

            case 0:
                // Assume redo log did not flush completely.
                return;

            case OP_TIMESTAMP:
                long ts = in.readLongLE();
                if (!verifyTerminator(in) || !visitor.timestamp(ts)) {
                    return;
                }
                break;

            case OP_SHUTDOWN:
                ts = in.readLongLE();
                if (!verifyTerminator(in) || !visitor.shutdown(ts)) {
                    return;
                }
                break;

            case OP_CLOSE:
                ts = in.readLongLE();
                if (!verifyTerminator(in) || !visitor.close(ts)) {
                    return;
                }
                break;

            case OP_END_FILE:
                ts = in.readLongLE();
                if (!verifyTerminator(in) || !visitor.endFile(ts)) {
                    return;
                }
                break;

            case OP_RESET:
                long txnId = in.readLongLE();
                if (!verifyTerminator(in) || !visitor.reset(mTxnId = txnId)) {
                    return;
                }
                break;

            case OP_TXN_ENTER:
                txnId = readTxnId(in);
                if (!verifyTerminator(in) || !visitor.txnEnter(txnId)) {
                    return;
                }
                break;

            case OP_TXN_ROLLBACK:
                txnId = readTxnId(in);
                if (!verifyTerminator(in) || !visitor.txnRollback(txnId)) {
                    return;
                }
                break;

            case OP_TXN_ROLLBACK_FINAL:
                txnId = readTxnId(in);
                if (!verifyTerminator(in) || !visitor.txnRollbackFinal(txnId)) {
                    return;
                }
                break;

            case OP_TXN_COMMIT:
                txnId = readTxnId(in);
                if (!verifyTerminator(in) || !visitor.txnCommit(txnId)) {
                    return;
                }
                break;

            case OP_TXN_COMMIT_FINAL:
                txnId = readTxnId(in);
                if (!verifyTerminator(in) || !visitor.txnCommitFinal(txnId)) {
                    return;
                }
                break;

            case OP_STORE:
                long indexId = in.readLongLE();
                byte[] key = in.readBytes();
                byte[] value = in.readBytes();
                if (!verifyTerminator(in) || !visitor.store(indexId, key, value)) {
                    return;
                }
                break;

            case OP_DELETE:
                indexId = in.readLongLE();
                key = in.readBytes();
                if (!verifyTerminator(in) || !visitor.store(indexId, key, null)) {
                    return;
                }
                break;

            case OP_TXN_ENTER_STORE:
                txnId = readTxnId(in);
                indexId = in.readLongLE();
                key = in.readBytes();
                value = in.readBytes();
                if (!verifyTerminator(in)
                    || !visitor.txnEnter(txnId)
                    || !visitor.txnStore(txnId, indexId, key, value))
                {
                    return;
                }
                break;

            case OP_TXN_STORE:
                txnId = readTxnId(in);
                indexId = in.readLongLE();
                key = in.readBytes();
                value = in.readBytes();
                if (!verifyTerminator(in) || !visitor.txnStore(txnId, indexId, key, value)) {
                    return;
                }
                break;

            case OP_TXN_STORE_COMMIT:
                txnId = readTxnId(in);
                indexId = in.readLongLE();
                key = in.readBytes();
                value = in.readBytes();
                if (!verifyTerminator(in)
                    || !visitor.txnStore(txnId, indexId, key, value)
                    || !visitor.txnCommit(txnId))
                {
                    return;
                }
                break;

            case OP_TXN_STORE_COMMIT_FINAL:
                txnId = readTxnId(in);
                indexId = in.readLongLE();
                key = in.readBytes();
                value = in.readBytes();
                if (!verifyTerminator(in)
                    || !visitor.txnStoreCommitFinal(txnId, indexId, key, value))
                {
                    return;
                }
                break;

            case OP_TXN_ENTER_DELETE:
                txnId = readTxnId(in);
                indexId = in.readLongLE();
                key = in.readBytes();
                if (!verifyTerminator(in)
                    || !visitor.txnEnter(txnId)
                    || !visitor.txnStore(txnId, indexId, key, null))
                {
                    return;
                }
                break;

            case OP_TXN_DELETE:
                txnId = readTxnId(in);
                indexId = in.readLongLE();
                key = in.readBytes();
                if (!verifyTerminator(in) || !visitor.txnStore(txnId, indexId, key, null)) {
                    return;
                }
                break;

            case OP_TXN_DELETE_COMMIT:
                txnId = readTxnId(in);
                indexId = in.readLongLE();
                key = in.readBytes();
                if (!verifyTerminator(in)
                    || !visitor.txnStore(txnId, indexId, key, null)
                    || !visitor.txnCommitFinal(txnId))
                {
                    return;
                }
                break;

            case OP_TXN_DELETE_COMMIT_FINAL:
                txnId = readTxnId(in);
                indexId = in.readLongLE();
                key = in.readBytes();
                if (!verifyTerminator(in)
                    || !visitor.txnStoreCommitFinal(txnId, indexId, key, null))
                {
                    return;
                }
                break;
            }
        }
    }

    private long readTxnId(DataIn in) throws IOException {
        return mTxnId += in.readSignedVarLong();
    }

    /**
     * If false is returned, assume rest of redo data is corrupt.
     * Implementation can return true if no redo terminators were written.
     */
    abstract boolean verifyTerminator(DataIn in) throws IOException;
}
