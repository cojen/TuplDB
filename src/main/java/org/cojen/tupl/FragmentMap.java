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

/**
 * Variation of FragmentCache which never evicts. Used by non-durable database.
 *
 * @author Brian S O'Neill
 */
class FragmentMap extends FragmentCache {
    private final IdHashTable<Node> mMap;

    FragmentMap() {
        mMap = new IdHashTable<>(16);
    }

    @Override
    Node get(Node caller, long nodeId) {
        Node node = mMap.get(Utils.scramble(nodeId));
        node.acquireShared();
        return node;
    }

    @Override
    void put(Node caller, Node node) {
        mMap.put(Utils.scramble(node.mId), node);
        node.mType = Node.TYPE_FRAGMENT;
    }

    @Override
    Node remove(Node caller, long nodeId) {
        Node node = mMap.remove(Utils.scramble(nodeId));
        node.acquireExclusive();
        return node;
    }
}
