/*
 *  Copyright (C) 2011-2017 Cojen.org
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.core;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

/**
 * Allows open BTrees to be unloaded when no longer in use.
 *
 * @author Brian S O'Neill
 */
final class BTreeRef extends WeakReference<BTree> {
    final long mId;
    final byte[] mName;
    final Node mRoot;

    /**
     * @param tree referenced tree
     */
    BTreeRef(BTree tree, ReferenceQueue<Object> queue) {
        super(tree, queue);
        mId = tree.mId;
        mName = tree.mName;
        mRoot = tree.mRoot;
    }
}
