/*
 *  Copyright 2011 Brian S O'Neill
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
 * 
 *
 * @author Brian S O'Neill
 */
class UndoLog {
    /*
      Key format:   txnId: long, seqId: int
      Value format: op: byte, indexId: long, keyLength: varInt, key: bytes, ...
    */

    private static final byte OP_STORE = 1, OP_DELETE = 2;

    /**
     * @param value value which is being replaced; pass null if none exists
     */
    public void undoStore(long txnId, long indexId, byte[] key, byte[] value) throws IOException {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }

        /*
        if (value == null) {
            writeOp(RedoLog.OP_STORE, txnId);
            writeLong(indexId);
            writeUnsignedVarInt(key.length);
            writeBytes(key);
        } else {
            writeOp(RedoLog.OP_DELETE, txnId);
            writeLong(indexId);
            writeUnsignedVarInt(key.length);
            writeBytes(key);
            writeUnsignedVarInt(value.length);
            writeBytes(value);
        }
        */

        // FIXME:
        throw null;
    }

    /**
     * Closes this log instance, discarding all entries.
     */
    public void commit() throws IOException {
        // FIXME:
        throw null;
    }

    /**
     * Closes this log instance and passes all log entries to the database
     * rollback handler, in reverse order.
     *
     * @throws IllegalStateException if log is closed
     */
    public void rollback() {
        // FIXME:
        throw null;
    }

    public static interface RollbackHandler {
        /**
         * Interpret the log entry and logically undo the encoded operation.
         * Undo operations must be idempotent, because they might be seen again
         * when the database is re-opened.
         *
         * @param entry non-null log entry data
         * @param offset offset to start of entry data
         * @param length length of entry, from offset
         */
        public void undo(Database db, byte[] entry, int offset, int length);
    }
}
