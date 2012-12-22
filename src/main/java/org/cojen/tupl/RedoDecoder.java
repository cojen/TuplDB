/*
 *  Copyright 2012 Brian S O'Neill
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
    RedoDecoder() {
    }

    /**
     * Reads from the given stream, passing operations to the visitor, until
     * the end of stream is reached or visitor returns false.
     */
    void run(DataIn in, RedoVisitor visitor) throws IOException {
        int op;
        while ((op = in.read()) >= 0) {
            long operand = in.readLongLE();

            switch (op &= 0xff) {
            default:
                throw new DatabaseException("Unknown redo log operation: " + op);

            case 0:
                // Assume redo log did not flush completely.
                return;

            case OP_TIMESTAMP:
                if (!verifyTerminator(in) || !visitor.timestamp(operand)) {
                    return;
                }
                break;

            case OP_SHUTDOWN:
                if (!verifyTerminator(in) || !visitor.shutdown(operand)) {
                    return;
                }
                break;

            case OP_CLOSE:
                if (!verifyTerminator(in) || !visitor.close(operand)) {
                    return;
                }
                break;

            case OP_END_FILE:
                if (!verifyTerminator(in) || !visitor.endFile(operand)) {
                    return;
                }
                break;

            case OP_TXN_BEGIN:
                if (!verifyTerminator(in) || !visitor.txnBegin(operand)) {
                    return;
                }
                break;

            case OP_TXN_BEGIN_CHILD:
                long parentTxnId = in.readLongLE();
                if (!verifyTerminator(in) || !visitor.txnBeginChild(operand, parentTxnId)) {
                    return;
                }
                break;

            case OP_TXN_ROLLBACK:
                if (!verifyTerminator(in) || !visitor.txnRollback(operand)) {
                    return;
                }
                break;

            case OP_TXN_ROLLBACK_CHILD:
                parentTxnId = in.readLongLE();
                if (!verifyTerminator(in) || !visitor.txnRollbackChild(operand, parentTxnId)) {
                    return;
                }
                break;

            case OP_TXN_COMMIT:
                if (!verifyTerminator(in) || !visitor.txnCommit(operand)) {
                    return;
                }
                break;

            case OP_TXN_COMMIT_CHILD:
                parentTxnId = in.readLongLE();
                if (!verifyTerminator(in) || !visitor.txnCommitChild(operand, parentTxnId)) {
                    return;
                }
                break;

            case OP_STORE:
                byte[] key = in.readBytes();
                byte[] value = in.readBytes();
                if (!verifyTerminator(in) || !visitor.store(operand, key, value)) {
                    return;
                }
                break;

            case OP_DELETE:
                key = in.readBytes();
                if (!verifyTerminator(in) || !visitor.store(operand, key, null)) {
                    return;
                }
                break;

            case OP_TXN_STORE:
                long indexId = in.readLongLE();
                key = in.readBytes();
                value = in.readBytes();
                if (!verifyTerminator(in) || !visitor.txnStore(operand, indexId, key, value)) {
                    return;
                }
                break;

            case OP_TXN_STORE_COMMIT:
                indexId = in.readLongLE();
                key = in.readBytes();
                value = in.readBytes();
                if (!verifyTerminator(in)
                    || !visitor.txnStoreCommit(operand, indexId, key, value))
                {
                    return;
                }
                break;

            case OP_TXN_STORE_COMMIT_CHILD:
                parentTxnId = in.readLongLE();
                indexId = in.readLongLE();
                key = in.readBytes();
                value = in.readBytes();
                if (!verifyTerminator(in)
                    || !visitor.txnStoreCommitChild(operand, parentTxnId, indexId, key, value))
                {
                    return;
                }
                break;

            case OP_TXN_DELETE:
                indexId = in.readLongLE();
                key = in.readBytes();
                if (!verifyTerminator(in) || !visitor.txnStore(operand, indexId, key, null)) {
                    return;
                }
                break;

            case OP_TXN_DELETE_COMMIT:
                indexId = in.readLongLE();
                key = in.readBytes();
                if (!verifyTerminator(in)
                    || !visitor.txnStoreCommit(operand, indexId, key, null))
                {
                    return;
                }
                break;

            case OP_TXN_DELETE_COMMIT_CHILD:
                parentTxnId = in.readLongLE();
                indexId = in.readLongLE();
                key = in.readBytes();
                if (!verifyTerminator(in)
                    || !visitor.txnStoreCommitChild(operand, parentTxnId, indexId, key, null))
                {
                    return;
                }
                break;
            }
        }
    }

    /**
     * If false is returned, assume rest of redo data is corrupt.
     * Implementation can return true if no redo terminators were written.
     */
    abstract boolean verifyTerminator(DataIn in) throws IOException;
}
