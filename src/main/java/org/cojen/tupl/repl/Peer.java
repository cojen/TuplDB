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
 * Peers are ordered by match position, for determining the commit position.
 *
 * @author Brian S O'Neill
 */
final class Peer implements Comparable<Peer> {
    private static final VarHandle
        cRoleHandle, cGroupVersionHandle, cSnapshotScoreHandle, cRangesHandle;

    static {
        try {
            cRoleHandle =
                MethodHandles.lookup().findVarHandle
                (Peer.class, "mRole", Role.class);

            cGroupVersionHandle =
                MethodHandles.lookup().findVarHandle
                (Peer.class, "mGroupVersion", long.class);

            cSnapshotScoreHandle =
                MethodHandles.lookup().findVarHandle
                (Peer.class, "mSnapshotScore", SnapshotScore.class);

            cRangesHandle =
                MethodHandles.lookup().findVarHandle
                (Peer.class, "mQueryRanges", RangeSet.class);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    final long mMemberId;
    final SocketAddress mAddress;

    private Role mRole;

    long mMatchPosition;

    long mSyncMatchPosition;

    volatile long mCompactPosition;

    volatile long mGroupVersion;

    private volatile SnapshotScore mSnapshotScore;

    // Used for election stability.
    volatile long mLeaderCheck;

    // Tracks data ranges requested by the peer.
    private volatile RangeSet mQueryRanges;

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

    Role role() {
        return (Role) cRoleHandle.getOpaque(this);
    }

    void role(Role role) {
        cRoleHandle.setOpaque(this, role);
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

    /**
     * @return non-null set if it was just created
     */
    RangeSet queryData(long startPosition, long endPosition) {
        RangeSet set = mQueryRanges;

        while (true) {
            RangeSet result = null;

            if (set == null) {
                set = new RangeSet();
                RangeSet existing = (RangeSet) cRangesHandle.compareAndExchange(this, null, set);
                if (existing == null) {
                    result = set;
                } else {
                    set = existing;
                }
            }

            if (set.add(startPosition, endPosition)) {
                return result;
            }

            set = finishedQueries(set);
        }
    }

    /**
     * @return non-null set if more queries must be handled
     */
    RangeSet finishedQueries(RangeSet set) {
        if (set.closeIfEmpty()) {
            RangeSet existing = (RangeSet) cRangesHandle.compareAndExchange(this, set, null);
            if (existing == set) {
                set = null;
            } else {
                set = existing;
            }
        }

        return set;
    }

    /**
     * To be called upon catching an exception while trying to handle query requests. New
     * requests can still be received, which will then create a new RangeSet.
     */
    void discardQueries(RangeSet set) {
        mQueryRanges = null;
        set.close();
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
            var other = (Peer) obj;
            return mMemberId == other.mMemberId && Objects.equals(mAddress, other.mAddress)
                && Objects.equals(mRole, other.mRole);
        }
        return false;
    }

    @Override
    public int compareTo(Peer other) {
        return Long.compare(mMatchPosition, other.mMatchPosition);
    }

    @Override
    public String toString() {
        return "Peer: {memberId=" + Long.toUnsignedString(mMemberId)
            + ", address=" + mAddress + ", role=" + mRole + '}';
    }
}
