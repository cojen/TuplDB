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

/**
 * 
 *
 * @author Brian S O'Neill
 */
class RedoOps {
    // Note: When updating the opcodes, be sure to update RedoWriter and RedoDecoder.

    static final byte
        /** timestamp: long */
        OP_TIMESTAMP = 1,

        /** timestamp: long */
        OP_SHUTDOWN = 2,

        /** timestamp: long */
        OP_CLOSE = 3,

        /** timestamp: long */
        OP_END_FILE = 4,

        /** txnId: long */
        OP_TXN_BEGIN = 5,

        /** txnId: long, parentTxnId: long */
        OP_TXN_BEGIN_CHILD = 6,

        /** txnId: long */
        //OP_TXN_CONTINUE = 7,

        /** txnId: long, parentTxnId: long */
        //OP_TXN_CONTINUE_CHILD = 8,

        /** txnId: long */
        OP_TXN_ROLLBACK = 9,

        /** txnId: long, parentTxnId: long */
        OP_TXN_ROLLBACK_CHILD = 10,

        /** txnId: long */
        OP_TXN_COMMIT = 11,

        /** txnId: long, parentTxnId: long */
        OP_TXN_COMMIT_CHILD = 12,

        /** indexId: long, keyLength: varInt, key: bytes, valueLength: varInt, value: bytes */
        OP_STORE = 16,

        /** indexId: long, keyLength: varInt, key: bytes */
        OP_DELETE = 17,

        /** indexId: long */
        //OP_CLEAR = 18,

        /** txnId: long, indexId: long, keyLength: varInt, key: bytes,
            valueLength: varInt, value: bytes */
        OP_TXN_STORE = 19,

        /** txnId: long, indexId: long, keyLength: varInt, key: bytes,
            valueLength: varInt, value: bytes */
        OP_TXN_STORE_COMMIT = 20,

        /** txnId: long, parentTxnId: long, indexId: long, keyLength: varInt, key: bytes,
            valueLength: varInt, value: bytes */
        OP_TXN_STORE_COMMIT_CHILD = 21,

        /** txnId: long, indexId: long, keyLength: varInt, key: bytes */
        OP_TXN_DELETE = 22,

        /** txnId: long, indexId: long, keyLength: varInt, key: bytes */
        OP_TXN_DELETE_COMMIT = 23,

        /** txnId: long, parentTxnId: long, indexId: long, keyLength: varInt, key: bytes */
        OP_TXN_DELETE_COMMIT_CHILD = 24;

        /** txnId: long, indexId: long */
        //OP_TXN_CLEAR = 25,

        /** txnId: long, indexId: long */
        //OP_TXN_CLEAR_COMMIT = 26,

        /** txnId: long, parentTxnId: long, indexId: long */
        //OP_TXN_CLEAR_COMMIT_CHILD = 27,

        /** length: varInt, data: bytes */
        //OP_CUSTOM = (byte) 128,

        /** txnId: long, length: varInt, data: bytes */
        //OP_TXN_CUSTOM = (byte) 129;
}
