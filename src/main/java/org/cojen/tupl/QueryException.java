/*
 *  Copyright (C) 2024 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl;

/**
 * Thrown when a query expression is malformed.
 *
 * @author Brian S. O'Neill
 */
public class QueryException extends IllegalArgumentException {
    private final int mStartPos, mEndPos;

    public QueryException(String message) {
        super(message);
        mStartPos = -1;
        mEndPos = -1;
    }

    public QueryException(Throwable cause) {
        super(cause);
        mStartPos = -1;
        mEndPos = -1;
    }

    public QueryException(String message, Throwable cause) {
        super(message, cause);
        mStartPos = -1;
        mEndPos = -1;
    }

    public QueryException(String message, int pos) {
        this(message, pos, pos + 1);
    }

    public QueryException(String message, int startPos, int endPos) {
        super(message);
        mStartPos = startPos;
        mEndPos = endPos;
    }

    public final String baseMessage() {
        return super.getMessage();
    }

    /**
     * @return source code start position, zero-based, inclusive; is -1 if not applicable
     */
    public final int startPos() {
        return mStartPos;
    }

    /**
     * @return source code end position, zero-based, exclusive; is -1 if not applicable
     */
    public final int endPos() {
        return mEndPos;
    }

    @Override
    public String getMessage() {
        String message = baseMessage();

        if (mStartPos < 0) {
            return message;
        }

        // FIXME: Add methods to augment the message with source code context.

        var b = new StringBuilder().append(message).append(" @");

        if (mStartPos + 1 < mEndPos) {
            b.append('[').append(mStartPos).append("..").append(mEndPos).append(')');
        } else {
            b.append(mStartPos);
        }

        return b.toString();
    }
}
