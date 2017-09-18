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
     * Normal members receive replicated data, provide consensus, cast votes, and they can
     * become the leader.
     */
    NORMAL((byte) 1),

    /**
     * Standby members receive replicated data, and they provide consensus. They don't cast
     * votes, and they cannot become the leader.
     */
    STANDBY((byte) 2),

    /**
     * Observers only receive replicated data. They don't provide consensus, they don't cast
     * votes, and they cannot become the leader.
     */
    OBSERVER((byte) 3);

    byte mCode;

    private Role(byte code) {
        mCode = code;
    }

    static Role decode(byte code) {
        switch (code) {
        case 1:
            return NORMAL;
        case 2:
            return STANDBY;
        case 3:
            return OBSERVER;
        default:
            throw new IllegalArgumentException();
        }
    }
}
