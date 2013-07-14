/*
 *  Copyright 2012-2013 Brian S O'Neill
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

import static org.cojen.tupl.Node.*;

/**
 * Special cache implementation for fragment nodes, as used by fragmented
 * values.
 *
 * @author Brian S O'Neill
 */
class FragmentCache {
    private final LHT[] mHashTables;
    private final int mHashTableShift;

    FragmentCache(Database db, int maxCapacity) {
        this(db, maxCapacity, Runtime.getRuntime().availableProcessors() * 16);
    }

    private FragmentCache(Database db, int maxCapacity, int numHashTables) {
        numHashTables = Utils.roundUpPower2(numHashTables);
        maxCapacity = (maxCapacity + numHashTables - 1) / numHashTables;
        mHashTables = new LHT[numHashTables];
        for (int i=0; i<numHashTables; i++) {
            mHashTables[i] = new LHT(db, maxCapacity);
        }
        mHashTableShift = Integer.numberOfLeadingZeros(numHashTables - 1);
    }

    protected FragmentCache() {
        mHashTables = null;
        mHashTableShift = 0;
    }

    /**
     * Returns the node with the given id, possibly loading it and evicting another.
     *
     * @param caller optional tree node which is latched and calling this method
     * @return node with shared latch held
     */
    Node get(Node caller, long nodeId) throws IOException {
        int hash = hash(nodeId);
        return mHashTables[hash >>> mHashTableShift].get(caller, nodeId, hash);
    }

    /**
     * Returns the node with the given id, possibly evicting another. Method is intended for
     * obtaining nodes to write into.
     *
     * @param caller optional tree node which is latched and calling this method
     * @param load true if node should be fully loaded
     * @return node with exclusive latch held
     */
    Node getw(Node caller, long nodeId, boolean load) throws IOException {
        int hash = hash(nodeId);
        return mHashTables[hash >>> mHashTableShift].getw(caller, nodeId, load, hash);
    }

    /**
     * Returns the node with the given id, if already in the cache. Method is intended for
     * obtaining nodes to write into.
     *
     * @param caller optional tree node which is latched and calling this method
     * @param parent optional parent inode
     * @return node with exclusive latch held, or null if not found
     */
    Node findw(Node caller, Node parent, long nodeId) {
        int hash = hash(nodeId);
        return mHashTables[hash >>> mHashTableShift].findw(caller, parent, nodeId, hash);
    }

    /**
     * Stores the node, and possibly evicts another. As a side-effect, node type is set to
     * TYPE_FRAGMENT. Node latch is not released, even if an exception is thrown.
     *
     * @param caller optional tree node which is latched and calling this method
     * @param node exclusively latched node
     */
    void put(Node caller, Node node) throws IOException {
        int hash = hash(node.mId);
        mHashTables[hash >>> mHashTableShift].put(caller, node, hash);
    }

    /**
     * @param caller optional tree node which is latched and calling this method
     * @return exclusively latched node if found; null if not found
     */
    Node remove(Node caller, long nodeId) {
        int hash = hash(nodeId);
        return mHashTables[hash >>> mHashTableShift].remove(caller, nodeId, hash);
    }

    static int hash(long nodeId) {
        int hash = ((int) nodeId) ^ ((int) (nodeId >>> 32));
        // Scramble the hashcode a bit, just like ConcurrentHashMap does.
        hash += (hash <<  15) ^ 0xffffcd7d;
        hash ^= (hash >>> 10);
        hash += (hash <<   3);
        hash ^= (hash >>>  6);
        hash += (hash <<   2) + (hash << 14);
        return hash ^ (hash >>> 16);
    }

    /**
     * Simple "lossy" hashtable of Nodes. When a collision is found, the
     * existing entry (if TYPE_FRAGMENT) might simply be evicted.
     */
    static final class LHT extends Latch {
        private static final float LOAD_FACTOR = 0.75f;

        private final Database mDatabase;
        private final int mMaxCapacity;

        private Node[] mEntries;
        private int mSize;
        private int mGrowThreshold;

        // Padding to prevent cache line sharing.
        private long a0, a1, a2;

        LHT(Database db, int maxCapacity) {
            // Initial capacity of must be a power of 2.
            mEntries = new Node[Utils.roundUpPower2(Math.min(16, maxCapacity))];
            mGrowThreshold = (int) (mEntries.length * LOAD_FACTOR);
            mDatabase = db;
            mMaxCapacity = maxCapacity;
        }

        /**
         * Returns the node with the given id, possibly loading it and evicting another.
         *
         * @param caller optional tree node which is latched and calling this method
         * @return node with shared latch held
         */
        Node get(final Node caller, final long nodeId, final int hash) throws IOException {
            acquireShared();
            boolean htEx = false;
            boolean nEx = false;

            while (true) {
                final Node[] entries = mEntries;
                final int index = hash & (entries.length - 1);
                Node existing = entries[index];
                int incr = 0;
                if (existing == null) {
                    incr = 1;
                } else {
                    if (existing == caller || existing.mType != TYPE_FRAGMENT) {
                        existing = null;
                    } else {
                        if (nEx) {
                            existing.acquireExclusive();
                        } else {
                            existing.acquireShared();
                        }
                        if (existing.mId == nodeId) {
                            release(htEx);
                            mDatabase.used(existing);
                            if (nEx) {
                                existing.downgrade();
                            }
                            return existing;
                        }
                    }
                }

                // Need to have an exclusive lock before making modifications to hashtable.
                if (!htEx) {
                    htEx = true;
                    if (!tryUpgrade()) {
                        if (existing != null) {
                            existing.release(nEx);
                        }
                        releaseShared();
                        acquireExclusive();
                        continue;
                    }
                }

                if (existing != null) {
                    if (existing.mType != TYPE_FRAGMENT) {
                        // Hashtable slot can be used without evicting anything.
                        existing.release(nEx);
                        existing = null;
                    } else if (rehash(caller, null, existing)) {
                        // See if rehash eliminates collision.
                        existing.release(nEx);
                        continue;
                    } else if (!nEx && !existing.tryUpgrade()) {
                        // Exclusive latch is required for eviction.
                        existing.releaseShared();
                        nEx = true;
                        continue;
                    }
                }

                mSize += incr;

                // Allocate node and reserve slot.
                final Node node = mDatabase.allocLatchedNode();
                node.mId = nodeId;
                node.mType = TYPE_FRAGMENT;
                entries[index] = node;

                // Evict and load without ht latch held.
                releaseExclusive();

                if (existing != null) {
                    try {
                        existing.doEvict(mDatabase.mPageDb);
                        existing.releaseExclusive();
                    } catch (IOException e) {
                        node.mId = 0;
                        node.releaseExclusive();
                        throw e;
                    }
                }

                node.mCachedState = mDatabase.readNodePage(nodeId, node.mPage);

                node.downgrade();
                return node;
            }
        }

        /**
         * Returns the node with the given id, possibly evicting another. Method is intended
         * for obtaining nodes to write into.
         *
         * @param caller optional tree node which is latched and calling this method
         * @param load true if node should be fully loaded
         * @return node with exclusive latch held
         */
        Node getw(final Node caller, final long nodeId, final boolean load, final int hash)
            throws IOException
        {
            acquireShared();
            boolean htEx = false;

            while (true) {
                final Node[] entries = mEntries;
                final int index = hash & (entries.length - 1);
                Node existing = entries[index];
                int incr = 0;
                if (existing == null) {
                    incr = 1;
                } else {
                    if (existing == caller || existing.mType != TYPE_FRAGMENT) {
                        existing = null;
                    } else {
                        existing.acquireExclusive();
                        if (existing.mId == nodeId) {
                            release(htEx);
                            mDatabase.used(existing);
                            return existing;
                        }
                    }
                }

                // Need to have an exclusive lock before making modifications to hashtable.
                if (!htEx) {
                    htEx = true;
                    if (!tryUpgrade()) {
                        if (existing != null) {
                            existing.releaseExclusive();
                        }
                        releaseShared();
                        acquireExclusive();
                        continue;
                    }
                }

                if (existing != null) {
                    if (existing.mType != TYPE_FRAGMENT) {
                        // Hashtable slot can be used without evicting anything.
                        existing.releaseExclusive();
                        existing = null;
                    } else if (rehash(caller, null, existing)) {
                        // See if rehash eliminates collision.
                        existing.releaseExclusive();
                        continue;
                    }
                }

                mSize += incr;

                // Allocate node and reserve slot.
                final Node node = mDatabase.allocLatchedNode();
                node.mId = nodeId;
                node.mType = TYPE_FRAGMENT;
                entries[index] = node;

                // Evict without ht latch held.
                releaseExclusive();

                if (existing != null) {
                    try {
                        existing.doEvict(mDatabase.mPageDb);
                        existing.releaseExclusive();
                    } catch (IOException e) {
                        node.mId = 0;
                        node.releaseExclusive();
                        throw e;
                    }
                }

                if (load) {
                    node.mCachedState = mDatabase.readNodePage(nodeId, node.mPage);
                }

                return node;
            }
        }

        /**
         * Returns the node with the given id, if already in the cache. Method is intended for
         * obtaining nodes to write into.
         *
         * @param caller optional tree node which is latched and calling this method
         * @param parent optional parent inode
         * @return node with exclusive latch held, or null if not found
         */
        Node findw(final Node caller, final Node parent, final long nodeId, final int hash) {
            acquireShared();
            final Node[] entries = mEntries;
            final int index = hash & (entries.length - 1);
            final Node existing = entries[index];
            if (existing != null && existing != caller && existing != parent &&
                existing.mType == TYPE_FRAGMENT)
            {
                existing.acquireExclusive();
                if (existing.mId == nodeId) {
                    releaseShared();
                    mDatabase.used(existing);
                    return existing;
                }
                existing.releaseExclusive();
            }
            releaseShared();
            return null;
        }

        /**
         * Stores the node, and possibly evicts another. As a side-effect, node type is set to
         * TYPE_FRAGMENT. Node latch is not released, even if an exception is thrown.
         *
         * @param caller optional tree node which is latched and calling this method
         * @param node exclusively latched node; evictable
         */
        void put(final Node caller, final Node node, final int hash) throws IOException {
            if (!tryAcquireExclusive()) {
                // Avoid deadlock caused by inconsistent LHT and Node lock ordering.
                mDatabase.makeUnevictable(node);
                node.releaseExclusive();
                this.acquireExclusive();
                node.acquireExclusive();
                mDatabase.makeEvictable(node);
            }

            while (true) {
                final Node[] entries = mEntries;
                final int index = hash & (entries.length - 1);
                Node existing = entries[index];
                if (existing == null) {
                    mSize++;
                } else {
                    if (existing == caller || existing == node
                        || existing.mType != TYPE_FRAGMENT)
                    {
                        existing = null;
                    } else {
                        existing.acquireExclusive();
                        if (existing.mType != TYPE_FRAGMENT) {
                            // Hashtable slot can be used without evicting anything.
                            existing.releaseExclusive();
                            existing = null;
                        } else if (rehash(caller, node, existing)) {
                            // See if rehash eliminates collision.
                            existing.releaseExclusive();
                            continue;
                        }
                    }
                }

                node.mType = TYPE_FRAGMENT;
                entries[index] = node;

                // Evict without ht latch held.
                releaseExclusive();

                if (existing != null) {
                    existing.doEvict(mDatabase.mPageDb);
                    existing.releaseExclusive();
                }

                return;
            }
        }

        Node remove(final Node caller, final long nodeId, final int hash) {
            acquireExclusive();

            Node[] entries = mEntries;
            int index = hash & (entries.length - 1);
            Node existing = entries[index];
            if (existing != null && existing != caller && existing.mId == nodeId) {
                existing.acquireExclusive();
                if (existing.mId == nodeId) {
                    entries[index] = null;
                    mSize--;
                    releaseExclusive();
                    return existing;
                }
                existing.releaseExclusive();
            }

            releaseExclusive();
            return null;
        }

        /**
         * Caller must hold exclusive latch.
         *
         * @param node optional latched node to be inserted
         * @param existing latched node in desired slot
         * @return false if not rehashed
         */
        private boolean rehash(Node caller, Node node, Node existing) {
            Node[] entries;
            int capacity;
            if (mSize < mGrowThreshold ||
                (capacity = (entries = mEntries).length) >= mMaxCapacity)
            {
                return false;
            }

            capacity <<= 1;
            Node[] newEntries = new Node[capacity];
            int newSize = 0;
            int newMask = capacity - 1;

            for (int i=entries.length, mask=entries.length-1; --i>=0 ;) {
                Node e = entries[i];
                if (e != null && e != caller) {
                    long id;
                    if (e == node) {
                        continue;
                    } else if (e == existing) {
                        if (e.mType != TYPE_FRAGMENT || ((hash(id = e.mId) & mask) != i)) {
                            continue;
                        }
                    } else {
                        e.acquireShared();
                        if (e.mType != TYPE_FRAGMENT || ((hash(id = e.mId) & mask) != i)) {
                            e.releaseShared();
                            continue;
                        }
                        e.releaseShared();
                    }

                    int index = hash(id) & newMask;

                    if (newEntries[index] != null) {
                        // Rehash should never create collisions.
                        throw new AssertionError();
                    }

                    newEntries[index] = e;
                    newSize++;
                }
            }

            mEntries = newEntries;
            mSize = newSize;
            mGrowThreshold = (int) (capacity * LOAD_FACTOR);

            return true;
        }
    }
}
