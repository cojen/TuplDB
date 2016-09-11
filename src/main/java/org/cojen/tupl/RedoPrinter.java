/*
 *  Copyright 2011-2015 Cojen.org
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
    public static void main(String[] args) throws Exception {
        java.io.File baseFile = new java.io.File(args[0]);
        long logId = Long.parseLong(args[1]);
        new RedoLog(null, baseFile, null, logId, 0, true)
            .replay(new RedoPrinter(), null, null, null);
    }

    private static final int MAX_VALUE = 1000;

    private final PrintStream mOut;

    RedoPrinter() {
        mOut = System.out;
    }

    @Override
    public boolean reset() {
        mOut.println("reset");
        return true;
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
    public boolean storeNoLock(long indexId, byte[] key, byte[] value) {
        mOut.println("storeNoLock: indexId=" + indexId +
                     ", key=" + toHex(key) + ", value=" + toHex(value));
        return true;
    }

    @Override
    public boolean renameIndex(long txnId, long indexId, byte[] newName) {
        mOut.println("renameIndex: txnId=" + txnId +
                     ", indexId=" + indexId + ", newName=" + toHex(newName));
        return true;
    }

    @Override
    public boolean deleteIndex(long txnId, long indexId) {
        mOut.println("deleteIndex: txnId=" + txnId + ", indexId=" + indexId);
        return true;
    }

    @Override
    public boolean txnEnter(long txnId) {
        mOut.println("txnEnter: txnId=" + txnId);
        return true;
    }

    @Override
    public boolean txnRollback(long txnId) {
        mOut.println("txnRollback: txnId=" + txnId);
        return true;
    }

    @Override
    public boolean txnRollbackFinal(long txnId) {
        mOut.println("txnRollbackFinal: txnId=" + txnId);
        return true;
    }

    @Override
    public boolean txnCommit(long txnId) {
        mOut.println("txnCommit: txnId=" + txnId);
        return true;
    }

    @Override
    public boolean txnCommitFinal(long txnId) {
        mOut.println("txnCommitFinal: txnId=" + txnId);
        return true;
    }

    @Override
    public boolean txnStore(long txnId, long indexId, byte[] key, byte[] value) {
        mOut.println("txnStore: txnId=" + txnId + ", indexId=" + indexId +
                     ", key=" + toHex(key) + ", value=" + toHex(value));
        return true;
    }

    @Override
    public boolean txnStoreCommitFinal(long txnId, long indexId, byte[] key, byte[] value) {
        txnStore(txnId, indexId, key, value);
        return txnCommit(txnId);
    }

    @Override
    public boolean txnCustom(long txnId, byte[] message) {
        mOut.println("txnCustom: txnId=" + txnId + ", message=" + toHex(message));
        return true;
    }

    @Override
    public boolean txnCustomLock(long txnId, byte[] message, long indexId, byte[] key) {
        mOut.println("txnCustomLock: txnId=" + txnId + ", message=" + toHex(message) +
                     ", indexId=" + indexId + ", key=" + toHex(key));
        return true;
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
        return java.time.Instant.ofEpochMilli(timestamp).toString();
    }
}
