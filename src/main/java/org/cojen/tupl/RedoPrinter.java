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
        new RedoLog(null, baseFile, null, logId, 0, null)
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
    public boolean control(byte[] message) {
        mOut.println("control: message=" + toHex(message));
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
    public boolean txnPrepare(long txnId) {
        mOut.println("txnPrepare: txnId=" + txnId);
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
    public boolean txnEnterStore(long txnId, long indexId, byte[] key, byte[] value) {
        txnEnter(txnId);
        txnStore(txnId, indexId, key, value);
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
    public boolean txnStoreCommitFinal(long txnId, long indexId, byte[] key, byte[] value) {
        txnStore(txnId, indexId, key, value);
        return txnCommitFinal(txnId);
    }

    @Override
    public boolean cursorRegister(long cursorId, long indexId) {
        mOut.println("cursorRegister: cursorId=" + cursorId + ", indexId=" + indexId);
        return true;
    }

    @Override
    public boolean cursorUnregister(long cursorId) {
        mOut.println("cursorUnregister: cursorId=" + cursorId);
        return true;
    }

    @Override
    public boolean cursorStore(long cursorId, long txnId, byte[] key, byte[] value) {
        mOut.println("cursorStore: cursorId=" + cursorId + ", txnId=" + txnId +
                     ", key=" + toHex(key) + ", value=" + toHex(value));
        return true;
    }

    @Override
    public boolean cursorFind(long cursorId, long txnId, byte[] key) {
        mOut.println("cursorFind: cursorId=" + cursorId + ", txnId=" + txnId +
                     ", key=" + toHex(key));
        return true;
    }

    @Override
    public boolean cursorValueSetLength(long cursorId, long txnId, long length) {
        mOut.println("cursorValueSetLength: cursorId=" + cursorId + ", txnId=" + txnId +
                     ", length=" + length);
        return true;
    }

    @Override
    public boolean cursorValueWrite(long cursorId, long txnId,
                                    long pos, byte[] buf, int off, int len)
    {
        mOut.println("cursorValueWrite: cursorId=" + cursorId + ", txnId=" + txnId +
                     ", pos=" + pos + ", value=" + toHex(buf, off, len));
        return true;
    }

    @Override
    public boolean cursorValueClear(long cursorId, long txnId, long pos, long length) {
        mOut.println("cursorValueClear: cursorId=" + cursorId + ", txnId=" + txnId +
                     ", pos=" + pos + ", length=" + length);
        return true;
    }

    @Override
    public boolean txnLockShared(long txnId, long indexId, byte[] key) {
        mOut.println("txnLockShared: txnId=" + txnId + ", indexId=" + indexId +
                     ", key=" + toHex(key));
        return true;
    }

    @Override
    public boolean txnLockUpgradable(long txnId, long indexId, byte[] key) {
        mOut.println("txnLockUpgradable: txnId=" + txnId + ", indexId=" + indexId +
                     ", key=" + toHex(key));
        return true;
    }

    @Override
    public boolean txnLockExclusive(long txnId, long indexId, byte[] key) {
        mOut.println("txnLockExclusive: txnId=" + txnId + ", indexId=" + indexId +
                     ", key=" + toHex(key));
        return true;
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

    private static String toHex(byte[] bytes) {
        return toHex(bytes, 0, bytes.length);
    }

    private static String toHex(byte[] bytes, int offset, int length) {
        if (bytes == null) {
            return "null";
        }
        StringBuilder bob;
        int len;
        if (length <= MAX_VALUE) {
            len = length;
            bob = new StringBuilder(len * 2);
        } else {
            len = MAX_VALUE;
            bob = new StringBuilder(len * 2 + 3);
        }
        for (int i=0; i<len; i++) {
            int b = bytes[offset + i] & 0xff;
            if (b < 16) {
                bob.append('0');
            }
            bob.append(Integer.toHexString(b));
        }
        if (length > MAX_VALUE) {
            bob.append("...").append(" (length=").append(length).append(')');
        }
        return bob.toString();
    }

    private static String toDateTime(long timestamp) {
        return java.time.Instant.ofEpochMilli(timestamp).toString();
    }
}
