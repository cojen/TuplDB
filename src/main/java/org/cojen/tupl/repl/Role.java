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

/**
 * Supported membership roles.
 *
 * @author Brian S O'Neill
 */
public enum Role {
    /**
     * Normal members receive replicated data, they can {@link ReplicatorConfig#proxyWrites
     * proxy writes}, they provide consensus, and they can become the leader.
     */
    NORMAL((byte) 1),

    /**
     * Standby members receive replicated data, they can {@link ReplicatorConfig#proxyWrites
     * proxy writes}, and they provide consensus. They can only become an interim leader, to
     * permit a normal member to catch up. An interim leader doesn't accept any new writes.
     */
    STANDBY((byte) 2),

    /**
     * Proxy members receive replicated data, and they can {@link ReplicatorConfig#proxyWrites
     * proxy writes}. They don't provide consensus, and they cannot become the leader.
     */
    PROXY((byte) 3),

    /**
     * Observers only receive replicated data. They don't {@link ReplicatorConfig#proxyWrites
     * proxy writes}, they don't provide consensus, and they cannot become the leader.
     */
    OBSERVER((byte) 4),

    /**
     * A restoring member is an observer which just joined the group and is receiving a
     * snapshot. If the restore fails, the member is automatically removed from the group.
     */
    RESTORING((byte) 5),

    /**
     * Voters only provide consensus. They don't receive replicated data, they don't {@link
     * ReplicatorConfig#proxyWrites proxy writes}, and they cannot become the leader.
     *
     * <p><i>Note: Experimental feature.</i>
     */
    VOTER((byte) -1);

    byte mCode;

    private Role(byte code) {
        mCode = code;
    }

    boolean providesConsensus() {
        return mCode <= 2;
    }

    boolean canProxy() {
        return mCode >= 1 && mCode <= 3;
    }

    boolean isCandidate() {
        return mCode >= 1 && mCode <= 2;
    }

    static Role decode(byte code) {
        switch (code) {
        case 1:
            return NORMAL;
        case 2:
            return STANDBY;
        case 3:
            return PROXY;
        case 4:
            return OBSERVER;
        case -1:
            return VOTER;
        default:
            throw new IllegalArgumentException();
        }
    }
}
