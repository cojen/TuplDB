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

import org.cojen.tupl.diag.EventType;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ErrorCodes {
    static final byte SUCCESS = 0, UNKNOWN_OPERATION = 1, NO_ACCEPTOR = 2,
        UNKNOWN_MEMBER = 3, UNCONNECTED_MEMBER = 4, INVALID_ADDRESS = 5,
        VERSION_MISMATCH = 6, NO_CONSENSUS = 7, NO_LEADER = 8, NOT_LEADER = 9;

    static String toString(byte errorCode) {
        return switch (errorCode) {
            default -> "unknown error: " + errorCode;
            case SUCCESS -> "success";
            case UNKNOWN_OPERATION -> "unknown operation";
            case NO_ACCEPTOR -> "no acceptor";
            case UNKNOWN_MEMBER -> "unknown member";
            case INVALID_ADDRESS -> "invalid address";
            case UNCONNECTED_MEMBER -> "unconnected member";
            case VERSION_MISMATCH -> "group version mismatch";
            case NO_CONSENSUS -> "no consensus";
            case NO_LEADER -> "no leader";
            case NOT_LEADER -> "not leader";
        };
    }

    static EventType typeFor(byte errorCode) {
        return switch (errorCode) {
            default -> EventType.REPLICATION_PANIC;
            case VERSION_MISMATCH, NO_CONSENSUS, NO_LEADER, NOT_LEADER
                -> EventType.REPLICATION_WARNING;
            case SUCCESS -> EventType.REPLICATION_INFO;
        };
    }
}
