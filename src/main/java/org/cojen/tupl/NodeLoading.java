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
 * Special marker class used by a loading node. Not really a split state, but
 * it allows the Node's split field to be re-used. A node cannot be split while
 * loading, and so there's no state conflict.
 *
 * @author Brian S O'Neill
 */
final class NodeLoading extends Split {
    /**
     * Initial marker, which is replaced later when await is called.
     */
    static final NodeLoading MARKER = new NodeLoading();

    WaitQueue mWaitQueue;

    private NodeLoading() {
        super(false, null);
    }

    /**
     * Wait for load signal, from a node known to be in a loading state. Method
     * never returns spuriously.
     *
     * @param node exclusively latched node which is loading; relatched when
     * method returns
     * @return queue to signal next waiter
     */
    static WaitQueue await(Node node) {
        NodeLoading loading = (NodeLoading) node.mSplit;

        WaitQueue queue;
        if (loading == MARKER) {
            loading = new NodeLoading();
            loading.mWaitQueue = queue = new WaitQueue();
            node.mSplit = loading;
        } else {
            queue = loading.mWaitQueue;
        }

        // Bind a cursor frame to prevent node eviction after latch is released
        // by await method.
        TreeCursorFrame frame = new TreeCursorFrame();
        frame.bind(node, 0);

        while (queue.await(node, new WaitQueue.Node(), -1, 0) <= 0);

        // Latch is held again, and so no eviction will occur.
        frame.unbind();

        return queue;
    }
}
