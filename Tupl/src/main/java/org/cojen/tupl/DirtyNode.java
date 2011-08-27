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
 * Reference to node which must be committed.
 *
 * @author Brian S O'Neill
 */
final class DirtyNode implements Comparable<DirtyNode> {
    TreeNode mNode;
    long mId;

    DirtyNode(TreeNode node, long id) {
        mNode = node;
        // Stable copy of id that can be accessed without re-latching.
        mId = id;
    }

    @Override
    public int compareTo(DirtyNode other) {
        long a = mId;
        long b = other.mId;
        return a < b ? -1 : (a > b ? 1 : 0);
    }
}
