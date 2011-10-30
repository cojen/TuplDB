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

import java.io.PrintStream;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class RedoLogPrinter implements RedoLogVisitor {
    private final PrintStream mOut;

    RedoLogPrinter() {
        mOut = System.out;
    }

    public void timestamp(long timestamp) {
        mOut.println("timestamp: " + toDateTime(timestamp));
    }

    public void shutdown(long timestamp) {
        mOut.println("shutdown: " + toDateTime(timestamp));
    }

    public void close(long timestamp) {
        mOut.println("close: " + toDateTime(timestamp));
    }

    public void endFile(long timestamp) {
        mOut.println("endFile: " + toDateTime(timestamp));
    }

    public void store(long indexId, byte[] key, byte[] value) {
        mOut.println("store: indexId=" + indexId +
                     ", key=" + toHex(key) + ", value=" + toHex(value));
    }

    public void clear(long indexId) {
        mOut.println("clear: indexId=" + indexId);
    }

    public void txnRollback(long txnId, long parentTxnId) {
        mOut.println("txnRollback: txnId=" + txnId + ", parentTxnId=" + parentTxnId);
    }

    public void txnCommit(long txnId, long parentTxnId) {
        mOut.println("txnCommit: txnId=" + txnId + ", parentTxnId=" + parentTxnId);
    }

    public void txnStore(long txnId, long indexId, byte[] key, byte[] value) {
        mOut.println("txnStore: txnId=" + txnId + ", indexId=" + indexId +
                     ", key=" + toHex(key) + ", value=" + toHex(value));
    }

    private String toHex(byte[] bytes) {
        StringBuilder bob = new StringBuilder(bytes.length * 2);
        for (int i=0; i<bytes.length; i++) {
            int b = bytes[i] & 0xff;
            if (b < 16) {
                bob.append('0');
            }
            bob.append(Integer.toHexString(b));
        }
        return bob.toString();
    }

    private String toDateTime(long timestamp) {
        return new java.util.Date(timestamp).toString();
    }
}
