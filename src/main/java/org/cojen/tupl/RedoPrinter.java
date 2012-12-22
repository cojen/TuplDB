/*
 *  Copyright 2011-2012 Brian S O'Neill
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

import java.io.PrintStream;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class RedoPrinter implements RedoVisitor {
    private static final int MAX_VALUE = 1000;

    private final PrintStream mOut;

    RedoPrinter() {
        mOut = System.out;
    }

    @Override
    public boolean timestamp(long timestamp) {
        mOut.println("timestamp: " + toDateTime(timestamp));
        return true;
    }

    @Override
    public boolean shutdown(long timestamp) {
        mOut.println("shutdown: " + toDateTime(timestamp));
        return true;
    }

    @Override
    public boolean close(long timestamp) {
        mOut.println("close: " + toDateTime(timestamp));
        return true;
    }

    @Override
    public boolean endFile(long timestamp) {
        mOut.println("endFile: " + toDateTime(timestamp));
        return true;
    }

    @Override
    public boolean store(long indexId, byte[] key, byte[] value) {
        mOut.println("store: indexId=" + indexId +
                     ", key=" + toHex(key) + ", value=" + toHex(value));
        return true;
    }

    @Override
    public boolean txnBegin(long txnId) {
        mOut.println("txnBegin: txnId=" + txnId);
        return true;
    }

    @Override
    public boolean txnBeginChild(long txnId, long parentTxnId) {
        mOut.println("txnBegin: txnId=" + txnId + ", parentTxnId=" + parentTxnId);
        return true;
    }

    @Override
    public boolean txnRollback(long txnId) {
        mOut.println("txnRollback: txnId=" + txnId);
        return true;
    }

    @Override
    public boolean txnRollbackChild(long txnId, long parentTxnId) {
        mOut.println("txnRollback: txnId=" + txnId + ", parentTxnId=" + parentTxnId);
        return true;
    }

    @Override
    public boolean txnCommit(long txnId) {
        mOut.println("txnCommit: txnId=" + txnId);
        return true;
    }

    @Override
    public boolean txnCommitChild(long txnId, long parentTxnId) {
        mOut.println("txnCommit: txnId=" + txnId + ", parentTxnId=" + parentTxnId);
        return true;
    }

    @Override
    public boolean txnStore(long txnId, long indexId, byte[] key, byte[] value) {
        mOut.println("txnStore: txnId=" + txnId + ", indexId=" + indexId +
                     ", key=" + toHex(key) + ", value=" + toHex(value));
        return true;
    }

    @Override
    public boolean txnStoreCommit(long txnId, long indexId, byte[] key, byte[] value) {
        txnStore(txnId, indexId, key, value);
        return txnCommit(txnId);
    }

    @Override
    public boolean txnStoreCommitChild(long txnId, long parentTxnId,
                                       long indexId, byte[] key, byte[] value)
    {
        txnStore(txnId, indexId, key, value);
        return txnCommitChild(txnId, parentTxnId);
    }

    private String toHex(byte[] bytes) {
        if (bytes == null) {
            return "null";
        }
        StringBuilder bob;
        int len;
        if (bytes.length <= MAX_VALUE) {
            len = bytes.length;
            bob = new StringBuilder(len * 2);
        } else {
            len = MAX_VALUE;
            bob = new StringBuilder(len * 2 + 3);
        }
        for (int i=0; i<len; i++) {
            int b = bytes[i] & 0xff;
            if (b < 16) {
                bob.append('0');
            }
            bob.append(Integer.toHexString(b));
        }
        if (bytes.length > MAX_VALUE) {
            bob.append("...");
        }
        return bob.toString();
    }

    private String toDateTime(long timestamp) {
        return new java.util.Date(timestamp).toString();
    }
}
