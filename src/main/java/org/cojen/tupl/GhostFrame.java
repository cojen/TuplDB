/*
 *  Copyright 2016 Cojen.org
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
 * Special frame type for tracking ghosted entries within leaf nodes. Unlike regular frames,
 * ghost frames don't prevent the bound node from being evicted.
 *
 * @author Brian S O'Neill
 */
/*P*/
final class GhostFrame extends CursorFrame {
    private static final byte[] EVICTED = new byte[0];

    GhostFrame() {
    }

    /**
     * Called with node latch held exclusively, when the node referenced by this frame has been
     * evicted.
     */
    void evicted() {
        mNotFoundKey = EVICTED;
        popAll(this);
    }

    boolean isEvicted() {
        return mNotFoundKey == EVICTED;
    }
}
