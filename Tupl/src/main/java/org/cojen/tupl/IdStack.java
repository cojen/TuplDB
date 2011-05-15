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

import java.util.NoSuchElementException;

/**
 * Growable stack of long ids.
 *
 * @author Brian S O'Neill
 */
class IdStack {
    private static final int NODE_SIZE = 1000;

    private Node mTail;
    private int mSize;

    public IdStack() {
        mTail = new Node(null);
    }

    public int size() {
        return mSize;
    }

    public void add(long id) {
        Node tail = mTail;
        int offset = tail.mOffset;
        if (offset >= NODE_SIZE) {
            mTail = tail = new Node(tail);
            offset = 0;
        }
        tail.mIds[offset++] = id;
        tail.mOffset = offset;
        mSize++;
    }

    public long remove() {
        int size = mSize;
        if (size <= 0) {
            throw new NoSuchElementException();
        }
        Node tail = mTail;
        int offset = tail.mOffset;
        if (offset <= 0) {
            mTail = tail = tail.mPrev;
            offset = tail.mOffset;
        }
        long result = tail.mIds[--offset];
        tail.mOffset = offset;
        mSize--;
        return result;
    }

    private static class Node {
        final Node mPrev;
        final long[] mIds;
        int mOffset;

        Node(Node prev) {
            mPrev = prev;
            mIds = new long[NODE_SIZE];
        }
    }
}
