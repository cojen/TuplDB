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

import java.util.HashMap;
import java.util.Map;
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
        cRoleHandle, cGroupVersionHandle, cRangesHandle, cCompactPositionHandle;

    static {
        try {
            var lookup = MethodHandles.lookup();

            cRoleHandle = lookup.findVarHandle(Peer.class, "mRole", Role.class);

            cGroupVersionHandle = lookup.findVarHandle(Peer.class, "mGroupVersion", long.class);

            cRangesHandle = lookup.findVarHandle(Peer.class, "mQueryRanges", RangeSet.class);

            cCompactPositionHandle = lookup.findVarHandle
                (Peer.class, "mCompactPosition", long.class);
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

    // Map requestedBy to async SnapshotScore requests.
    private Map<Object, SnapshotScore> mSnapshotScores;

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

    void updateCompactPosition(long position) {
        while (true) {
            long currentPosition = mCompactPosition;
            position = Math.max(currentPosition, position);
            if (position <= currentPosition ||
                cCompactPositionHandle.compareAndSet(this, currentPosition, position))
            {
                return;
            }
        }
    }

    void updateGroupVersion(final long groupVersion) {
        while (true) {
            long currentVersion = mGroupVersion;
            if (groupVersion <= currentVersion ||
                cGroupVersionHandle.compareAndSet(this, currentVersion, groupVersion))
            {
                return;
            }
        }
    }

    /**
     * @param requestedBy unique instance (can be current thread)
     */
    synchronized void prepareSnapshotScore(Object requestedBy) {
        if (mSnapshotScores == null) {
            mSnapshotScores = new HashMap<>(2);
        }
        mSnapshotScores.put(requestedBy, new SnapshotScore(this));
    }

    /**
     * Must call prepareSnapshotScore first.
     *
     * @return null if timed out
     */
    SnapshotScore awaitSnapshotScore(Object requestedBy, long timeoutMillis) {
        SnapshotScore score;
        synchronized (this) {
            score = mSnapshotScores.get(requestedBy);
        }

        if (score == null) {
            throw new IllegalStateException();
        }

        try {
            if (!score.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
                score = null;
            }
        } catch (Throwable e) {
            score = null;
        }

        synchronized (this) {
            mSnapshotScores.remove(requestedBy);
            if (mSnapshotScores.isEmpty()) {
                mSnapshotScores = null;
            }
        }

        return score;
    }

    synchronized void snapshotScoreReply(int activeSessions, float weight) {
        if (mSnapshotScores != null) {
            mSnapshotScores.values().forEach(s -> s.snapshotScoreReply(activeSessions, weight));
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
                var existing = (RangeSet) cRangesHandle.compareAndExchange(this, null, set);
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
            var existing = (RangeSet) cRangesHandle.compareAndExchange(this, set, null);
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
        return obj == this || obj instanceof Peer other
            && mMemberId == other.mMemberId && Objects.equals(mAddress, other.mAddress)
            && Objects.equals(mRole, other.mRole);
    }

    @Override
    public int compareTo(Peer other) {
        return Long.compare(mMatchPosition, other.mMatchPosition);
    }

    @Override
    public String toString() {
        return "Peer{memberId=" + Long.toUnsignedString(mMemberId)
            + ", address=" + mAddress + ", role=" + mRole + '}';
    }
}
