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
 * List of nodes which must be committed.
 *
 * @author Brian S O'Neill
 */
final class DirtyList {
    final Block mHead;
    Block mTail;
    int mSize;

    DirtyList() {
        (mHead = mTail = new Block()).mLastPos = -1;
    }

    /**
     * Append node which is latched.
     */
    void append(Node node) {
        Block tail = mTail;
        Node[] nodes = tail.mNodes;
        int lastPos = tail.mLastPos + 1;
        if (lastPos < nodes.length) {
            nodes[lastPos] = node;
            tail.mIds[lastPos] = node.mId;
            tail.mLastPos = lastPos;
        } else {
            Block newTail = new Block();
            tail.mNext = newTail;
            newTail.mNodes[0] = node;
            newTail.mIds[0] = node.mId;
            mTail = newTail;
        }
        mSize++;
    }

    /**
     * Returns a copy of all the nodes, sorted by id.
     *
     * @return null if list is empty
     */
    Node[] sorted() {
        if (mSize == 0) {
            return null;
        }

        Node[] nodes = new Node[mSize];
        long[] ids = new long[mSize];

        DirtyList.Block iterator = mHead;
        int iteratorPos = 0;

        for (int i=0; i<nodes.length; i++) {
            nodes[i] = iterator.mNodes[iteratorPos];
            ids[i] = iterator.mIds[iteratorPos];
            if (++iteratorPos > iterator.mLastPos) {
                if ((iterator = iterator.mNext) == null) {
                    break;
                }
                iteratorPos = 0;
            }
        }

        sort(nodes, ids, 0, nodes.length);
        return nodes;
    }

    private static void sort(Node[] nodes, long[] ids, int off, int len) {
        // Note: Sort code is modified form of old java.util.Arrays sort. Using
        // the built-in sort would require each element to be wrapped with
        // micro objects. Node could be made Comparable, but the sort would
        // need to latch all Nodes to ensure that the ids don't change.

        // Insertion sort on smallest arrays
        if (len < 7) {
            for (int i=off; i<len+off; i++) {
                for (int j=i; j>off && ids[j-1]>ids[j]; j--) {
                    swap(nodes, ids, j, j-1);
                }
            }
            return;
        }

        // Choose a partition element, v
        int m = off + (len >> 1);       // Small arrays, middle element
        if (len > 7) {
            int l = off;
            int n = off + len - 1;
            if (len > 40) {        // Big arrays, pseudomedian of 9
                int s = len/8;
                l = med3(ids, l,     l+s, l+2*s);
                m = med3(ids, m-s,   m,   m+s);
                n = med3(ids, n-2*s, n-s, n);
            }
            m = med3(ids, l, m, n); // Mid-size, med of 3
        }
        long v = ids[m];

        // Establish Invariant: v* (<v)* (>v)* v*
        int a = off, b = a, c = off + len - 1, d = c;
        while (true) {
            while (b <= c && ids[b] <= v) {
                if (ids[b] == v) {
                    swap(nodes, ids, a++, b);
                }
                b++;
            }
            while (c >= b && ids[c] >= v) {
                if (ids[c] == v) {
                    swap(nodes, ids, c, d--);
                }
                c--;
            }
            if (b > c) {
                break;
            }
            swap(nodes, ids, b++, c--);
        }

        // Swap partition elements back to middle
        int s, n = off + len;
        s = Math.min(a-off, b-a  );  vecswap(nodes, ids, off, b-s, s);
        s = Math.min(d-c,   n-d-1);  vecswap(nodes, ids, b,   n-s, s);

        // Recursively sort non-partition-elements
        if ((s = b-a) > 1) {
            sort(nodes, ids, off, s);
        }
        if ((s = d-c) > 1) {
            sort(nodes, ids, n-s, s);
        }
    }

    private static void swap(Node[] nodes, long[] ids, int a, int b) {
        Node node = nodes[a];
        long id = ids[a];
        nodes[a] = nodes[b];
        ids[a] = ids[b];
        nodes[b] = node;
        ids[b] = id;
    }

    private static void vecswap(Node[] nodes, long[] ids, int a, int b, int n) {
        for (int i=0; i<n; i++, a++, b++) {
            swap(nodes, ids, a, b);
        }
    }

    /**
     * Returns the index of the median of the three indexed longs.
     */
    private static int med3(long x[], int a, int b, int c) {
        return (x[a] < x[b] ?
                (x[b] < x[c] ? b : x[a] < x[c] ? c : a) :
                (x[b] > x[c] ? b : x[a] > x[c] ? c : a));
    }

    static final class Block {
        final Node[] mNodes = new Node[100];
        // Stable copy of ids that can be accessed without re-latching.
        final long[] mIds = new long[100];
        int mLastPos;
        Block mNext;
    }
}
