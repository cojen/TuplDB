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

import java.net.SocketAddress;

/**
 * Peers are ordered by match index, for determining the commit index.
 *
 * @author Brian S O'Neill
 */
final class Peer implements Comparable<Peer> {
    final long mMemberId;
    final SocketAddress mAddress;

    long mMatchIndex;

    /**
     * Construct a key for finding peers in a set ordered by member id.
     */
    Peer(long memberId) {
        mMemberId = memberId;
        mAddress = null;
    }

    Peer(long memberId, SocketAddress addr) {
        if (memberId == 0 || addr == null) {
            throw new IllegalArgumentException();
        }
        mMemberId = memberId;
        mAddress = addr;
    }

    @Override
    public int compareTo(Peer other) {
        return Long.compare(mMatchIndex, other.mMatchIndex);
    }

    @Override
    public String toString() {
        return "Peer: {memberId=" + mMemberId + ", address=" + mAddress + '}';
    }
}
