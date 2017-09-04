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
 * 
 *
 * @author Brian S O'Neill
 */
final class ErrorCodes {
    static final byte SUCCESS = 0, UNKNOWN_OPERATION = 1, NO_ACCEPTOR = 2,
        UNKNOWN_MEMBER = 3, UNCONNECTED_MEMBER = 4,
        VERSION_MISMATCH = 5, NO_CONSENSUS = 6, NO_LEADER = 7, NOT_LEADER = 8;
}
