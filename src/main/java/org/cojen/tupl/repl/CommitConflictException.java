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

package org.cojen.tupl.repl;

import java.io.IOException;

/**
 * Thrown by StateLog when a term conflict was detected but cannot be resolved because the
 * local commit position cannot be rolled back.
 *
 * @author Brian S O'Neill
 */
class CommitConflictException extends IOException {
    final long mPosition;
    final LogInfo mTermInfo;
    final long mDurablePosition;

    CommitConflictException(long position, LogInfo termInfo, long durablePosition) {
        super("Fatal=" + isFatal(position, durablePosition) +
              ", position=" + position + ", " + termInfo + ", durable=" + durablePosition);
        mPosition = position;
        mTermInfo = termInfo;
        mDurablePosition = durablePosition;
    }

    /**
     * Returns true if the conflicting position is lower than the durable position, which
     * implies that a restart won't fix the conflict. When false is returned, a restart might
     * fix the conflict because non-durable changes can be rolled back.
     */
    boolean isFatal() {
        return isFatal(mPosition, mDurablePosition);
    }

    private static boolean isFatal(long position, long durablePosition) {
        return position < durablePosition;
    }
}
