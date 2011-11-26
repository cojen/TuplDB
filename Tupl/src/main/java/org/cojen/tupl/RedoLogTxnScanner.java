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

/**
 * Examines all logged transactions and discovers which ones were committed and
 * which ones will be rolled back.
 *
 * @author Brian S O'Neill
 */
class RedoLogTxnScanner implements RedoLogVisitor {
    private final LHashTable<Status> mStatusTable = new LHashTable<Status>(1024) {
        protected Status newEntry() {
            return new Status();
        }
    };

    private long mHighestTxnId;

    /**
     * Can only be called after performing scan.
     */
    public boolean isCommitted(long txnId) {
        Status status = mStatusTable.get(txnId);
        return status != null && status.mCommitted;
    }

    public long highestTxnId() {
        return mHighestTxnId;
    }

    @Override
    public void timestamp(long timestamp) {}

    @Override
    public void shutdown(long timestamp) {}

    @Override
    public void close(long timestamp) {}

    @Override
    public void endFile(long timestamp) {}

    @Override
    public void store(long indexId, byte[] key, byte[] value) {}

    @Override
    public void clear(long indexId) {}

    @Override
    public void txnRollback(long txnId, long parentTxnId) {
        checkHighest(txnId, parentTxnId);

        Status status = mStatusTable.remove(txnId);
        if (status != null) {
            if (parentTxnId != 0) {
                // Remove child transaction from parent, preventing full commit.
                Status parent = mStatusTable.get(parentTxnId);
                if (parent != null) {
                    parent.removeChild(txnId);
                }
            }

            // Rollback all child transactions.
            int count = status.mChildCount;
            if (count > 0) {
                long[] children = status.mChildren;
                for (int i=count; --i>=0; ) {
                    txnRollback(children[i], txnId);
                }
            }
        }
    }

    @Override
    public void txnCommit(long txnId, long parentTxnId) {
        checkHighest(txnId, parentTxnId);

        if (parentTxnId == 0) {
            fullCommit(txnId);
        } else {
            // Child transaction is not fully committed until parent is.
            mStatusTable.insert(parentTxnId).addChild(txnId);
        }
    }

    private void fullCommit(long txnId) {
        Status status = mStatusTable.insert(txnId);
        status.mCommitted = true;
        // Commit all child transactions.
        int count = status.mChildCount;
        if (count > 0) {
            long[] children = status.mChildren;
            for (int i=count; --i>=0; ) {
                fullCommit(children[i]);
            }
        }
    }

    @Override
    public void txnStore(long txnId, long indexId, byte[] key, byte[] value) {
        checkHighest(txnId);
    }

    private void checkHighest(long txnId, long parentTxnId) {
        checkHighest(txnId);
        checkHighest(parentTxnId);
    }

    private void checkHighest(long txnId) {
        if (txnId != 0) {
            long highestTxnId = mHighestTxnId;
            // Subtract for modulo comparison.
            if (highestTxnId == 0 || (txnId - highestTxnId) > 0) {
                mHighestTxnId = txnId;
            }
        }
    }

    static class Status extends LHashTable.Entry<Status> {
        boolean mCommitted;
        long[] mChildren;
        int mChildCount;

        void addChild(long txnId) {
            long[] children = mChildren;
            int count;
            if (children == null) {
                mChildren = children = new long[2]; // initial capacity
                count = 0;
            } else if ((count = mChildCount) >= children.length) {
                long[] newChildren = new long[children.length * 2];
                System.arraycopy(children, 0, newChildren, 0, count);
                mChildren = children = newChildren;
            }
            children[count] = txnId;
            mChildCount = count + 1;
        }

        void removeChild(long txnId) {
            long[] children = mChildren;
            if (children != null) {
                int count = mChildCount;
                for (int i=count; --i>=0; ) {
                    if (children[i] == txnId) {
                        System.arraycopy(children, i + 1, children, i, count - i - 1);
                        mChildCount = count - 1;
                        return;
                    }
                }
            }
        }
    }
}
