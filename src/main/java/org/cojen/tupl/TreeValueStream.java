/*
 *  Copyright 2013 Brian S O'Neill
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

import java.util.concurrent.locks.Lock;

import static org.cojen.tupl.Utils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class TreeValueStream extends Stream {
    // Op ordinals are relevant.
    private static final int OP_LENGTH = 0, OP_READ = 1, OP_SET_LENGTH = 2, OP_WRITE = 3;

    private final TreeCursor mCursor;

    /**
     * @param cursor positioned cursor, not autoloading
     */
    TreeValueStream(TreeCursor cursor) {
        mCursor = cursor;
    }

    @Override
    public LockResult open(Transaction txn, byte[] key) throws IOException {
        TreeCursor cursor = mCursor;
        if (cursor.key() != null) {
            close();
        }
        cursor.link(txn);
        try {
            return cursor.find(key);
        } catch (Throwable e) {
            mCursor.reset();
            throw rethrow(e);
        }
    }

    @Override
    public long length() throws IOException {
        return action(OP_LENGTH, 0, null, 0, 0);
    }

    @Override
    public void setLength(long length) throws IOException {
        // FIXME: txn undo/redo
        final Lock sharedCommitLock = mCursor.mTree.mDatabase.sharedCommitLock();
        sharedCommitLock.lock();
        try {
            action(OP_SET_LENGTH, length, null, 0, 0);
        } finally {
            sharedCommitLock.unlock();
        }
    }

    @Override
    int doRead(long pos, byte[] buf, int off, int len) throws IOException {
        return (int) action(OP_READ, pos, buf, off, len);
    }

    @Override
    int doWrite(long pos, byte[] buf, int off, int len) throws IOException {
        // FIXME: txn undo/redo
        final Lock sharedCommitLock = mCursor.mTree.mDatabase.sharedCommitLock();
        sharedCommitLock.lock();
        try {
            return (int) action(OP_WRITE, pos, buf, off, len);
        } finally {
            sharedCommitLock.unlock();
        }
    }

    @Override
    int pageSize() {
        return mCursor.mTree.mDatabase.pageSize();
    }

    @Override
    void checkOpen() {
        if (mCursor.key() == null) {
            throw new IllegalStateException("Stream closed");
        }
    }

    @Override
    void doClose() {
        mCursor.reset();
    }

    private long action(int op, long pos, byte[] b, int bOff, int bLen) throws IOException {
        TreeCursorFrame frame;
        if (op <= OP_READ) {
            frame = mCursor.leafSharedNotSplit();
        } else {
            // For this reason, write operations must acquire shared commit lock.
            frame = mCursor.leafExclusiveNotSplitDirty();
        }

        Node node = frame.mNode;

        int nodePos = frame.mNodePos;
        if (nodePos < 0) {
            if (op <= OP_READ) {
                node.releaseShared();
                return -1;
            }
            // FIXME: write ops; create the value
            node.releaseExclusive();
            throw null;
        }

        final byte[] page = node.mPage;
        int loc = readUnsignedShortLE(page, node.mSearchVecStart + nodePos);
        int header = page[loc++];
        loc += (header >= 0 ? header : (((header & 0x3f) << 8) | (page[loc] & 0xff))) + 1;

        long vLen;

        header = page[loc++];
        if (header >= 0) {
            vLen = header;
        } else {
            int len;
            if ((header & 0x20) == 0) {
                len = 1 + (((header & 0x1f) << 8) | (page[loc++] & 0xff));
            } else if (header != -1) {
                len = 1 + (((header & 0x0f) << 16)
                           | ((page[loc++] & 0xff) << 8) | (page[loc++] & 0xff));
            } else {
                // ghost
                if (op <= OP_READ) {
                    node.releaseShared();
                    return -1;
                }
                // FIXME: write ops; create the value
                node.releaseExclusive();
                throw null;
            }

            if ((header & Node.VALUE_FRAGMENTED) == 0) {
                vLen = len;
            } else {
                // Operate against a fragmented value. First read the fragment header, as
                // described by the Database.fragment method.
                header = page[loc++];

                switch ((header >> 2) & 0x03) {
                default:
                    vLen = readUnsignedShortLE(page, loc);
                    break;
                case 1:
                    vLen = readIntLE(page, loc) & 0xffffffffL;
                    break;
                case 2:
                    vLen = readUnsignedInt48LE(page, loc);
                    break;
                case 3:
                    vLen = readLongLE(page, loc);
                    if (vLen < 0) {
                        if (op <= OP_READ) {
                            node.releaseShared();
                        } else {
                            node.releaseExclusive();
                        }
                        throw new LargeValueException(vLen);
                    }
                    break;
                }

                // Advance past the value length field.
                loc += 2 + ((header >> 1) & 0x06);

                switch (op) {
                case OP_LENGTH: default:
                    node.releaseShared();
                    return vLen;

                case OP_READ: {
                    if (pos >= vLen) {
                        node.releaseShared();
                        return bLen == 0 ? 0 : -1;
                    }

                    bLen = Math.min((int) (vLen - pos), bLen);
                    final int total = bLen;

                    if ((header & 0x02) != 0) {
                        // Inline content.
                        int inLen = readUnsignedShortLE(page, loc);
                        loc += 2;
                        int amt = (int) (inLen - pos);
                        if (amt <= 0) {
                            // Not reading any inline content.
                            pos -= inLen;
                        } else if (bLen <= amt) {
                            System.arraycopy(page, (int) (loc + pos), b, bOff, bLen);
                            node.releaseShared();
                            return bLen;
                        } else {
                            System.arraycopy(page, (int) (loc + pos), b, bOff, amt);
                            bLen -= amt;
                            bOff += amt;
                            pos = 0;
                        }
                        loc += inLen;
                    }

                    final FragmentCache fc = mCursor.mTree.mDatabase.mFragmentCache;

                    if ((header & 0x01) == 0) {
                        // Direct pointers.
                        int ipos = (int) pos;
                        loc += (ipos / page.length) * 6;
                        int fNodeOff = ipos % page.length;
                        while (true) {
                            int amt = Math.min(bLen, page.length - fNodeOff);
                            long fNodeId = readUnsignedInt48LE(page, loc);
                            Node fNode = fc.get(node, fNodeId);
                            try {
                                System.arraycopy(fNode.mPage, fNodeOff, b, bOff, amt);
                                bLen -= amt;
                                if (bLen <= 0) {
                                    node.releaseShared();
                                    return total;
                                }
                            } finally {
                                fNode.releaseShared();
                            }
                            bOff += amt;
                            loc += 6;
                            fNodeOff = 0;
                        }
                    }
                         
                    // Indirect pointers.
                    // FIXME
                    throw null;
                }

                case OP_SET_LENGTH:
                    // FIXME
                    node.releaseExclusive();
                    throw null;

                case OP_WRITE:
                    // FIXME
                    node.releaseExclusive();
                    throw null;
                }
            }
        }

        // Operate against a non-fragmented value.

        switch (op) {
        case OP_LENGTH: default:
            node.releaseShared();
            return vLen;

        case OP_READ:
            if (pos >= vLen) {
                if (bLen > 0) {
                    bLen = -1;
                }
            } else {
                bLen = Math.min((int) (vLen - pos), bLen);
                System.arraycopy(page, (int) (loc + pos), b, bOff, bLen);
            }
            node.releaseShared();
            return bLen;

        case OP_SET_LENGTH:
            // FIXME
            node.releaseExclusive();
            throw null;

        case OP_WRITE:
            // FIXME
            node.releaseExclusive();
            throw null;
        }
    }
}
