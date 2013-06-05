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

/**
 * 
 *
 * @author Brian S O'Neill
 */
class RedoOps {
    // Note: When updating the opcodes, be sure to update RedoWriter and RedoDecoder.

    static final byte
        /** no operands */
        OP_RESET = 1,

        /** timestamp: long */
        OP_TIMESTAMP = 2,

        /** timestamp: long */
        OP_SHUTDOWN = 3,

        /** timestamp: long */
        OP_CLOSE = 4,

        /** timestamp: long */
        OP_END_FILE = 5,

        /** random: long */
        OP_NOP_RANDOM = 6,

        /** indexId: long, keyLength: varInt, key: bytes, valueLength: varInt, value: bytes */
        OP_STORE = 16,

        /** indexId: long, keyLength: varInt, key: bytes */
        OP_STORE_NO_LOCK = 17,

        /** indexId: long, keyLength: varInt, key: bytes, valueLength: varInt, value: bytes */
        OP_DELETE = 18,

        /** indexId: long, keyLength: varInt, key: bytes */
        OP_DELETE_NO_LOCK = 19,

        /** indexId: long */
        OP_DROP_INDEX = 20,

        /** txnId: delta */
        OP_TXN_ENTER = 24,

        /** txnId: delta */
        OP_TXN_ROLLBACK = 25,

        /** txnId: delta */
        OP_TXN_ROLLBACK_FINAL = 26,

        /** txnId: delta */
        OP_TXN_COMMIT = 27,

        /** txnId: delta */
        OP_TXN_COMMIT_FINAL = 28,

        /** txnId: delta, indexId: long, keyLength: varInt, key: bytes,
            valueLength: varInt, value: bytes */
        OP_TXN_ENTER_STORE = 32,

        /** txnId: delta, indexId: long, keyLength: varInt, key: bytes,
            valueLength: varInt, value: bytes */
        OP_TXN_STORE = 33,

        /** txnId: delta, indexId: long, keyLength: varInt, key: bytes,
            valueLength: varInt, value: bytes */
        OP_TXN_STORE_COMMIT = 34,

        /** txnId: delta, indexId: long, keyLength: varInt, key: bytes,
            valueLength: varInt, value: bytes */
        OP_TXN_STORE_COMMIT_FINAL = 35,

        /** txnId: delta, indexId: long, keyLength: varInt, key: bytes */
        OP_TXN_ENTER_DELETE = 36,

        /** txnId: delta, indexId: long, keyLength: varInt, key: bytes */
        OP_TXN_DELETE = 37,

        /** txnId: delta, indexId: long, keyLength: varInt, key: bytes */
        OP_TXN_DELETE_COMMIT = 38,

        /** txnId: delta, indexId: long, keyLength: varInt, key: bytes */
        OP_TXN_DELETE_COMMIT_FINAL = 39;

        /** length: varInt, data: bytes */
        //OP_CUSTOM = (byte) 128,

        /** txnId: long, length: varInt, data: bytes */
        //OP_TXN_CUSTOM = (byte) 129;
}
