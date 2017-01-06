/*
 *  Copyright 2012-2015 Cojen.org
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

import java.io.EOFException;
import java.io.IOException;

import static org.cojen.tupl.RedoOps.*;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see RedoWriter
 */
abstract class RedoDecoder {
    private final boolean mLenient;

    long mTxnId;

    RedoDecoder(boolean lenient, long initialTxnId) {
        mLenient = lenient;
        mTxnId = initialTxnId;
    }

    /**
     * Reads from the stream, passing operations to the visitor, until the end
     * of stream is reached or visitor returns false.
     *
     * @return true if end of stream reached; false if visitor returned false
     */
    @SuppressWarnings("fallthrough")
    boolean run(RedoVisitor visitor) throws IOException {
        while (true) {
            // Must be called before each operation, for the benefit of subclasses.
            DataIn in = in();

            int op = in.read();
            if (op < 0) {
                return true;
            }

            switch (op &= 0xff) {
            case 0:
                if (mLenient) {
                    // Assume redo log did not flush completely.
                    return true;
                }
                // fallthrough to next case...

            default:
                throw new DatabaseException
                    ("Unknown redo log operation: " + op + " at " + (in.mPos - 1));

            case OP_RESET:
                mTxnId = 0;
                if (!verifyTerminator(in)) {
                    return false;
                }
                if (!visitor.reset()) {
                    return false;
                }
                break;

            case OP_TIMESTAMP:
                long ts;
                try {
                    ts = in.readLongLE();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in) || !visitor.timestamp(ts)) {
                    return false;
                }
                break;

            case OP_SHUTDOWN:
                try {
                    ts = in.readLongLE();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in) || !visitor.shutdown(ts)) {
                    return false;
                }
                break;

            case OP_CLOSE:
                try {
                    ts = in.readLongLE();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in) || !visitor.close(ts)) {
                    return false;
                }
                break;

            case OP_END_FILE:
                try {
                    ts = in.readLongLE();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in) || !visitor.endFile(ts)) {
                    return false;
                }
                break;

            case OP_NOP_RANDOM:
                try {
                    in.readLongLE();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in)) {
                    return false;
                }
                break;

            case OP_TXN_ID_RESET:
                long txnId;
                try {
                    txnId = in.readLongLE();
                } catch (EOFException e) {
                    return true;
                }
                mTxnId = txnId;
                if (!verifyTerminator(in)) {
                    return false;
                }
                break;

            case OP_TXN_ENTER:
                try {
                    txnId = readTxnId(in);
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in) || !visitor.txnEnter(txnId)) {
                    return false;
                }
                break;

            case OP_TXN_ROLLBACK:
                try {
                    txnId = readTxnId(in);
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in) || !visitor.txnRollback(txnId)) {
                    return false;
                }
                break;

            case OP_TXN_ROLLBACK_FINAL:
                try {
                    txnId = readTxnId(in);
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in) || !visitor.txnRollbackFinal(txnId)) {
                    return false;
                }
                break;

            case OP_TXN_COMMIT:
                try {
                    txnId = readTxnId(in);
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in) || !visitor.txnCommit(txnId)) {
                    return false;
                }
                break;

            case OP_TXN_COMMIT_FINAL:
                try {
                    txnId = readTxnId(in);
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in) || !visitor.txnCommitFinal(txnId)) {
                    return false;
                }
                break;

            case OP_STORE:
                long indexId;
                byte[] key, value;
                try {
                    indexId = in.readLongLE();
                    key = in.readBytes();
                    value = in.readBytes();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in) || !visitor.store(indexId, key, value)) {
                    return false;
                }
                break;

            case OP_STORE_NO_LOCK:
                try {
                    indexId = in.readLongLE();
                    key = in.readBytes();
                    value = in.readBytes();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in) || !visitor.storeNoLock(indexId, key, value)) {
                    return false;
                }
                break;

            case OP_DELETE:
                try {
                    indexId = in.readLongLE();
                    key = in.readBytes();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in) || !visitor.store(indexId, key, null)) {
                    return false;
                }
                break;

            case OP_DELETE_NO_LOCK:
                try {
                    indexId = in.readLongLE();
                    key = in.readBytes();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in) || !visitor.storeNoLock(indexId, key, null)) {
                    return false;
                }
                break;

            case OP_RENAME_INDEX:
                byte[] newName;
                try {
                    txnId = readTxnId(in);
                    indexId = in.readLongLE();
                    newName = in.readBytes();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in) || !visitor.renameIndex(txnId, indexId, newName)) {
                    return false;
                }
                break;

            case OP_DELETE_INDEX:
                try {
                    txnId = readTxnId(in);
                    indexId = in.readLongLE();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in) || !visitor.deleteIndex(txnId, indexId)) {
                    return false;
                }
                break;

            case OP_TXN_ENTER_STORE:
                try {
                    txnId = readTxnId(in);
                    indexId = in.readLongLE();
                    key = in.readBytes();
                    value = in.readBytes();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in)
                    || !visitor.txnEnter(txnId)
                    || !visitor.txnStore(txnId, indexId, key, value))
                {
                    return false;
                }
                break;

            case OP_TXN_STORE:
                try {
                    txnId = readTxnId(in);
                    indexId = in.readLongLE();
                    key = in.readBytes();
                    value = in.readBytes();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in) || !visitor.txnStore(txnId, indexId, key, value)) {
                    return false;
                }
                break;

            case OP_TXN_STORE_COMMIT:
                try {
                    txnId = readTxnId(in);
                    indexId = in.readLongLE();
                    key = in.readBytes();
                    value = in.readBytes();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in)
                    || !visitor.txnStore(txnId, indexId, key, value)
                    || !visitor.txnCommit(txnId))
                {
                    return false;
                }
                break;

            case OP_TXN_STORE_COMMIT_FINAL:
                try {
                    txnId = readTxnId(in);
                    indexId = in.readLongLE();
                    key = in.readBytes();
                    value = in.readBytes();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in)
                    || !visitor.txnStoreCommitFinal(txnId, indexId, key, value))
                {
                    return false;
                }
                break;

            case OP_TXN_ENTER_DELETE:
                try {
                    txnId = readTxnId(in);
                    indexId = in.readLongLE();
                    key = in.readBytes();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in)
                    || !visitor.txnEnter(txnId)
                    || !visitor.txnStore(txnId, indexId, key, null))
                {
                    return false;
                }
                break;

            case OP_TXN_DELETE:
                try {
                    txnId = readTxnId(in);
                    indexId = in.readLongLE();
                    key = in.readBytes();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in) || !visitor.txnStore(txnId, indexId, key, null)) {
                    return false;
                }
                break;

            case OP_TXN_DELETE_COMMIT:
                try {
                    txnId = readTxnId(in);
                    indexId = in.readLongLE();
                    key = in.readBytes();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in)
                    || !visitor.txnStore(txnId, indexId, key, null)
                    || !visitor.txnCommitFinal(txnId))
                {
                    return false;
                }
                break;

            case OP_TXN_DELETE_COMMIT_FINAL:
                try {
                    txnId = readTxnId(in);
                    indexId = in.readLongLE();
                    key = in.readBytes();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in)
                    || !visitor.txnStoreCommitFinal(txnId, indexId, key, null))
                {
                    return false;
                }
                break;

            case (OP_TXN_CUSTOM & 0xff):
                byte[] message;
                try {
                    txnId = readTxnId(in);
                    message = in.readBytes();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in) || !visitor.txnCustom(txnId, message)) {
                    return false;
                }
                break;

            case (OP_TXN_CUSTOM_LOCK & 0xff):
                try {
                    txnId = readTxnId(in);
                    indexId = in.readLongLE();
                    key = in.readBytes();
                    message = in.readBytes();
                } catch (EOFException e) {
                    return true;
                }
                if (!verifyTerminator(in)
                    || !visitor.txnCustomLock(txnId, message, indexId, key))
                {
                    return false;
                }
                break;
            }
        }
    }

    long readTxnId(DataIn in) throws IOException {
        return mTxnId += in.readSignedVarLong();
    }

    /**
     * Invoked before each operation is read.
     */
    abstract DataIn in();

    /**
     * If false is returned, assume rest of redo data is corrupt.
     * Implementation can return true if no redo terminators were written.
     */
    abstract boolean verifyTerminator(DataIn in) throws IOException;
}
