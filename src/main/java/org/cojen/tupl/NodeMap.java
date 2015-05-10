/*
 *  Copyright 2014 Brian S O'Neill
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
final class NodeMap {
    private final Node[] mTable;
    private final Latch[] mLatches;

    NodeMap(int capacity) {
        this(capacity, Runtime.getRuntime().availableProcessors() * 16);
    }

    private NodeMap(int capacity, int latches) {
        latches = Utils.roundUpPower2(latches);
        capacity = Utils.roundUpPower2(capacity);
        if (capacity < 0) {
            capacity = 0x40000000;
        }
        mTable = new Node[capacity];
        mLatches = new Latch[latches];
        for (int i=0; i<latches; i++) {
            mLatches[i] = new Latch();
        }
    }

    static int hash(final long nodeId) {
        int hash = ((int) nodeId) ^ ((int) (nodeId >>> 32));
        return hash;
    }

    /**
     * Returns unconfirmed node if found. Caller must latch and confirm that node identifier
     * matches, in case an eviction snuck in.
     */
    Node get(final long nodeId) {
        return get(nodeId, hash(nodeId));
    }

    /**
     * Returns unconfirmed node if found. Caller must latch and confirm that node identifier
     * matches, in case an eviction snuck in.
     */
    Node get(final long nodeId, final int hash) {
        // Quick check without acquiring a partition latch.

        final Node[] table = mTable;
        Node node = table[hash & (table.length - 1)];
        if (node != null) {
            // Limit scan of collision chain in case a temporary infinite loop is observed.
            int limit = 100;
            do {
                if (node.mId == nodeId) {
                    return node;
                }
            } while ((node = node.mNodeChainNext) != null && --limit != 0);
        }

        // Again with shared partition latch held.

        final Latch[] latches = mLatches;
        final Latch latch = latches[hash & (latches.length - 1)];
        latch.acquireShared();

        node = table[hash & (table.length - 1)];
        while (node != null) {
            if (node.mId == nodeId) {
                latch.releaseShared();
                return node;
            }
            node = node.mNodeChainNext;
        }

        latch.releaseShared();
        return null;
    }

    /**
     * Put a node into the map, but caller must confirm that node is not already present.
     */
    void put(final Node node) {
        put(node, hash(node.mId));
    }

    /**
     * Put a node into the map, but caller must confirm that node is not already present.
     */
    void put(final Node node, final int hash) {
        final Latch[] latches = mLatches;
        final Latch latch = latches[hash & (latches.length - 1)];
        latch.acquireExclusive();

        final Node[] table = mTable;
        final int index = hash & (table.length - 1);
        Node e = table[index];
        while (e != null) {
            if (e == node) {
                latch.releaseExclusive();
                return;
            }
            if (e.mId == node.mId) {
                latch.releaseExclusive();
                throw new AssertionError();
            }
            e = e.mNodeChainNext;
        }

        node.mNodeChainNext = table[index];
        table[index] = node;

        latch.releaseExclusive();
    }

    void remove(final Node node, final int hash) {
        final Latch[] latches = mLatches;
        final Latch latch = latches[hash & (latches.length - 1)];
        latch.acquireExclusive();

        final Node[] table = mTable;
        final int index = hash & (table.length - 1);
        Node e = table[index];
        if (e == node) {
            table[index] = e.mNodeChainNext;
        } else while (e != null) {
            Node next = e.mNodeChainNext;
            if (next == node) {
                e.mNodeChainNext = next.mNodeChainNext;
                break;
            }
            e = next;
        }

        node.mNodeChainNext = null;

        latch.releaseExclusive();
    }

    /**
     * Remove and delete nodes from map, as part of close sequence.
     */
    void delete() {
        for (Latch latch : mLatches) {
            latch.acquireExclusive();
        }

        for (int i=mTable.length; --i>=0; ) {
            Node e = mTable[i];
            if (e != null) {
                e.delete();
                Node next;
                while ((next = e.mNodeChainNext) != null) {
                    e.mNodeChainNext = null;
                    e = next;
                }
                mTable[i] = null;
            }
        }

        for (Latch latch : mLatches) {
            latch.releaseExclusive();
        }
    }
}
