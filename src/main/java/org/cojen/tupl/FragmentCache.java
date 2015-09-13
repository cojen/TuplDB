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
 * Cache access methods for fragment nodes, as used by fragmented values.
 *
 * @author Brian S O'Neill
 */
final class FragmentCache {
    private final Database mDatabase;

    FragmentCache(Database db) {
        mDatabase = db;
    }

    /**
     * Returns or loads the node with the given id. If loaded, node is put in the cache.
     *
     * @return node with shared latch held
     */
    Node get(long nodeId) throws IOException {
        Node node = mDatabase.nodeMapGet(nodeId);

        if (node != null) {
            node.acquireShared();
            if (nodeId == node.mId) {
                node.used();
                return node;
            }
            node.releaseShared();
        }

        node = mDatabase.allocLatchedNode(nodeId);
        node.mId = nodeId;
        node.mType = TYPE_FRAGMENT;

        node.mCachedState = mDatabase.readNodePage(nodeId, node.mPage);
        node.downgrade();

        mDatabase.nodeMapPut(node);

        return node;
    }

    /**
     * Returns or loads the node with the given id. If loaded, node is put in the cache. Method
     * is intended for obtaining nodes to write into.
     *
     * @param load true if node should be fully loaded
     * @return node with exclusive latch held
     */
    Node getw(long nodeId, boolean load) throws IOException {
        Node node = mDatabase.nodeMapGet(nodeId);

        if (node != null) {
            node.acquireExclusive();
            if (nodeId == node.mId) {
                node.used();
                return node;
            }
            node.releaseExclusive();
        }

        node = mDatabase.allocLatchedNode(nodeId);
        node.mId = nodeId;
        node.mType = TYPE_FRAGMENT;

        if (load) {
            node.mCachedState = mDatabase.readNodePage(nodeId, node.mPage);
        }

        mDatabase.nodeMapPut(node);

        return node;
    }

    /**
     * Stores the node, and sets the type to TYPE_FRAGMENT. Node latch is not released, even if
     * an exception is thrown. Caller must confirm that node is not already present.
     *
     * @param node exclusively latched node
     */
    void put(Node node) {
        mDatabase.nodeMapPut(node);
        node.mType = TYPE_FRAGMENT;
    }

    /**
     * @return exclusively latched node if found; null if not found
     */
    Node remove(long nodeId) {
        int hash = Utils.hash(nodeId);
        Node node = mDatabase.nodeMapGet(nodeId, hash);
        if (node != null) {
            node.acquireExclusive();
            if (nodeId != node.mId) {
                node.releaseExclusive();
                node = null;
            } else {
                mDatabase.nodeMapRemove(node, hash);
            }
        }
        return node;
    }
}
