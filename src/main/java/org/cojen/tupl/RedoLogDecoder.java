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

import static org.cojen.tupl.RedoLog.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class RedoLogDecoder {
    RedoLogDecoder() {
    }

    /**
     * Reads from the given stream, passing operations to the visitor, until
     * the end of stream is reached.
     */
    void run(DataIn in, RedoLogVisitor visitor) throws IOException {
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
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.timestamp(operand);
                break;

            case OP_SHUTDOWN:
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.shutdown(operand);
                break;

            case OP_CLOSE:
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.close(operand);
                break;

            case OP_END_FILE:
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.endFile(operand);
                break;

            case OP_TXN_ROLLBACK:
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.txnRollback(operand);
                break;

            case OP_TXN_ROLLBACK_CHILD:
                long parentTxnId = in.readLongLE();
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.txnRollbackChild(operand, parentTxnId);
                break;

            case OP_TXN_COMMIT:
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.txnCommit(operand);
                break;

            case OP_TXN_COMMIT_CHILD:
                parentTxnId = in.readLongLE();
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.txnCommitChild(operand, parentTxnId);
                break;

            case OP_STORE:
                byte[] key = in.readBytes();
                byte[] value = in.readBytes();
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.store(operand, key, value);
                break;

            case OP_DELETE:
                key = in.readBytes();
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.store(operand, key, null);
                break;

            case OP_TXN_STORE:
                long indexId = in.readLongLE();
                key = in.readBytes();
                value = in.readBytes();
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.txnStore(operand, indexId, key, value);
                break;

            case OP_TXN_STORE_COMMIT:
                indexId = in.readLongLE();
                key = in.readBytes();
                value = in.readBytes();
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.txnStore(operand, indexId, key, value);
                visitor.txnCommit(operand);
                break;

            case OP_TXN_STORE_COMMIT_CHILD:
                parentTxnId = in.readLongLE();
                indexId = in.readLongLE();
                key = in.readBytes();
                value = in.readBytes();
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.txnStore(operand, indexId, key, value);
                visitor.txnCommitChild(operand, parentTxnId);
                break;

            case OP_TXN_DELETE:
                indexId = in.readLongLE();
                key = in.readBytes();
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.txnStore(operand, indexId, key, null);
                break;

            case OP_TXN_DELETE_COMMIT:
                indexId = in.readLongLE();
                key = in.readBytes();
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.txnStore(operand, indexId, key, null);
                visitor.txnCommit(operand);
                break;

            case OP_TXN_DELETE_COMMIT_CHILD:
                parentTxnId = in.readLongLE();
                indexId = in.readLongLE();
                key = in.readBytes();
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.txnStore(operand, indexId, key, null);
                visitor.txnCommitChild(operand, parentTxnId);
                break;
            }
        }
    }

    /**
     * If false is returned, assume rest of log file is corrupt. Default
     * implementation does nothing but return true.
     */
    boolean verifyTerminator(DataIn in) throws IOException {
        return true;
    }
}
