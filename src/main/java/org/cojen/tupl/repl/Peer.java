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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.net.SocketAddress;

import java.util.Objects;

import java.util.concurrent.TimeUnit;

import static org.cojen.tupl.io.Utils.rethrow;

/**
 * Peers are ordered by match index, for determining the commit index.
 *
 * @author Brian S O'Neill
 */
final class Peer implements Comparable<Peer> {
    private static final VarHandle cGroupVersionHandle, cSnapshotScoreHandle;

    static {
        try {
            cGroupVersionHandle =
                MethodHandles.lookup().findVarHandle
                (Peer.class, "mGroupVersion", long.class);

            cSnapshotScoreHandle =
                MethodHandles.lookup().findVarHandle
                (Peer.class, "mSnapshotScore", SnapshotScore.class);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    final long mMemberId;
    final SocketAddress mAddress;

    Role mRole;

    long mMatchIndex;

    long mSyncMatchIndex;

    volatile long mCompactIndex;

    volatile long mGroupVersion;

    private volatile SnapshotScore mSnapshotScore;

    /**
     * Construct a key for finding peers in an ordered set.
     */
    Peer(long memberId) {
        mMemberId = memberId;
        mAddress = null;
    }

    Peer(long memberId, SocketAddress addr, Role role) {
        if (memberId == 0 || addr == null || role == null) {
            throw new IllegalArgumentException();
        }
        mMemberId = memberId;
        mAddress = addr;
        mRole = role;
    }

    long updateGroupVersion(final long groupVersion) {
        while (true) {
            long currentVersion = mGroupVersion;
            if (groupVersion <= currentVersion ||
                cGroupVersionHandle.compareAndSet(this, currentVersion, groupVersion))
            {
                return currentVersion;
            }
        }
    }

    void resetSnapshotScore(SnapshotScore score) {
        mSnapshotScore = score;
    }

    SnapshotScore awaitSnapshotScore(Object requestedBy, long timeoutMillis) {
        SnapshotScore score = mSnapshotScore;

        if (score != null) {
            final SnapshotScore waitFor = score;

            try {
                if (!waitFor.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
                    score = null;
                }
            } catch (InterruptedException e) {
            }

            if (waitFor.mRequestedBy == requestedBy) {
                cSnapshotScoreHandle.compareAndSet(this, waitFor, null);
            }
        }

        return score;
    }

    void snapshotScoreReply(int activeSessions, float weight) {
        SnapshotScore score = mSnapshotScore;
        if (score != null) {
            score.snapshotScoreReply(activeSessions, weight);
        }
    }

    @Override
    public int hashCode() {
        return Long.hashCode(mMemberId) + Objects.hashCode(mAddress) + Objects.hashCode(mRole);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof Peer) {
            Peer other = (Peer) obj;
            return mMemberId == other.mMemberId && Objects.equals(mAddress, other.mAddress)
                && Objects.equals(mRole, other.mRole);
        }
        return false;
    }

    @Override
    public int compareTo(Peer other) {
        return Long.compare(mMatchIndex, other.mMatchIndex);
    }

    @Override
    public String toString() {
        return "Peer: {memberId=" + Long.toUnsignedString(mMemberId)
            + ", address=" + mAddress + ", role=" + mRole + '}';
    }
}
