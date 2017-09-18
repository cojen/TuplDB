/*
 *  Copyright (C) 2017 Cojen.org
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

package org.cojen.tupl.repl;

import java.util.concurrent.CountDownLatch;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class SnapshotScore extends CountDownLatch implements Comparable<SnapshotScore> {
    final Object mRequestedBy;
    final Channel mChannel;

    int mActiveSessions;
    float mWeight;

    SnapshotScore(Object requestedBy, Channel channel) {
        super(1);
        mRequestedBy = requestedBy;
        mChannel = channel;
    }

    void snapshotScoreReply(int activeSessions, float weight) {
        mActiveSessions = activeSessions;
        mWeight = weight;
        countDown();
    }

    @Override
    public int compareTo(SnapshotScore other) {
        int cmp = Integer.compare(mActiveSessions, other.mActiveSessions);
        if (cmp == 0) {
            cmp = Float.compare(mWeight, other.mWeight);
        }
        return cmp;
    }
}
