/*
 *  Copyright 2019 Cojen.org
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

import java.io.Serializable;

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;

import org.cojen.tupl.diag.DeadlockInfo;

/**
 * Use a special class instead of Set.of(...) to avoid duplicate entry detection, which throws
 * an exception. If for some reason the set has duplicates, that's much better than losing the
 * deadlock information altogether.
 *
 * @author Brian S O'Neill
 * @see DeadlockDetector
 */
final class DeadlockInfoSet extends AbstractSet<DeadlockInfo> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final DeadlockInfo[] mInfos;

    DeadlockInfoSet(DeadlockInfo[] infos) {
        mInfos = infos;
    }

    @Override
    public Iterator<DeadlockInfo> iterator() {
        return Arrays.asList(mInfos).iterator();
    }

    @Override
    public int size() {
        return mInfos.length;
    }
}
