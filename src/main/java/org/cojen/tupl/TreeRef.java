/*
 *  Copyright 2012-2015 Cojen.org
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

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

/**
 * Allows open Trees to be unloaded when no longer in use.
 *
 * @author Brian S O'Neill
 */
/*P*/
final class TreeRef extends WeakReference<Tree> {
    final long mId;
    final byte[] mName;
    final Node mRoot;

    TreeRef(Tree tree, ReferenceQueue<? super Tree> queue) {
        super(tree, queue);
        mId = tree.mId;
        mName = tree.mName;
        mRoot = tree.mRoot;
    }
}
