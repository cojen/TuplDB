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

/**
 * 
 *
 * @author Brian S O'Neill
 */
class RedoOps {
    // Note: When updating the opcodes, be sure to update RedoDecoder, TransactionContext, and
    // RedoWriter.

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

        /** txnId: long */
        OP_TXN_ID_RESET = 7,

        /** messageLength: varInt, message: bytes */
        OP_CONTROL = 8,

        /** indexId: long, keyLength: varInt, key: bytes, valueLength: varInt, value: bytes */
        OP_STORE = 16,

        /** indexId: long, keyLength: varInt, key: bytes */
        OP_STORE_NO_LOCK = 17,

        /** indexId: long, keyLength: varInt, key: bytes, valueLength: varInt, value: bytes */
        OP_DELETE = 18,

        /** indexId: long, keyLength: varInt, key: bytes */
        OP_DELETE_NO_LOCK = 19,

        /** txnId: delta, indexId: long */
        //OP_DROP_INDEX = 20, deprecated

        /** txnId: delta, indexId: long, nameLength: varInt, name: bytes */
        OP_RENAME_INDEX = 21,

        /** txnId: delta, indexId: long */
        OP_DELETE_INDEX = 22,

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

        /** txnId: delta, indexId: long, keyLength: varInt, key: bytes */
        OP_TXN_LOCK_SHARED = 29,

        /** txnId: delta, indexId: long, keyLength: varInt, key: bytes */
        OP_TXN_LOCK_UPGRADABLE = 30,

        /** txnId: delta, indexId: long, keyLength: varInt, key: bytes */
        OP_TXN_LOCK_EXCLUSIVE = 31,

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
        OP_TXN_DELETE_COMMIT_FINAL = 39,

        /** txnId: delta, dataLength: varInt, data: bytes */
        OP_TXN_CUSTOM = (byte) 128,

        /** txnId: delta, indexId: long, keyLength: varInt, key: bytes,
            dataLength: varInt, data: bytes */
        OP_TXN_CUSTOM_LOCK = (byte) 129;
}
