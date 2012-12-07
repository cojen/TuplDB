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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Variation of FragmentCache which never evicts. Used by non-durable database.
 *
 * @author Brian S O'Neill
 */
class FragmentMap extends FragmentCache {
    private final LHashTable.Obj<Node> mMap;
    private final ReadWriteLock mLock;

    // TODO: Is it worth increasing the concurrency with partitions?

    FragmentMap() {
        mMap = new LHashTable.Obj<Node>(16);
        mLock = new ReentrantReadWriteLock();
    }

    @Override
    Node get(Node caller, long nodeId) {
        Lock lock = mLock.readLock();
        lock.lock();
        try {
            Node node = mMap.get(nodeId).value;
            node.acquireShared();
            return node;
        } finally {
            lock.unlock();
        }
    }

    @Override
    void put(Node caller, Node node) {
        Lock lock = mLock.writeLock();
        lock.lock();
        try {
            mMap.replace(node.mId).value = node;
            node.mType = Node.TYPE_FRAGMENT;
        } finally {
            lock.unlock();
        }
    }

    @Override
    Node remove(Node caller, long nodeId) {
        Lock lock = mLock.writeLock();
        lock.lock();
        try {
            Node node = mMap.remove(nodeId).value;
            node.acquireExclusive();
            return node;
        } finally {
            lock.unlock();
        }
    }
}
