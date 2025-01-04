/*
 *  Copyright (C) 2011-2017 Cojen.org
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

package org.cojen.tupl;

/**
 * Describes the ordering of views, cursors, and tables.
 *
 * @author Brian S O'Neill
 */
public enum Ordering {
    /**
     * Describes ascending order, with nulls ordered high (will logically appear last).
     */
    ASCENDING,

    /**
     * Describes descending order, with nulls ordered high (will logically appear first).
     */
    DESCENDING,

    /**
     * Describes ascending order, with nulls ordered low (will logically appear first);
     */
    ASCENDING_NL,

    /**
     * Describes descending order, with nulls ordered low (will logically appear last);
     */
    DESCENDING_NL,

    UNSPECIFIED;

    public boolean isAscending() {
        return this == ASCENDING || this == ASCENDING_NL;
    }

    public boolean isDescending() {
        return this == DESCENDING || this == DESCENDING_NL;
    }

    public boolean areNullsHigh() {
        return this == ASCENDING || this == DESCENDING;
    }

    public boolean areNullsLow() {
        return this == ASCENDING_NL || this == DESCENDING_NL;
    }

    public boolean areNullsFirst() {
        return this == DESCENDING || this == ASCENDING_NL;
    }

    public boolean areNullsLast() {
        return this == ASCENDING || this == DESCENDING_NL;
    }

    public Ordering nullsHigh() {
        return switch (this) {
            case ASCENDING_NL -> ASCENDING;
            case DESCENDING_NL -> DESCENDING;
            default -> this;
        };
    }

    public Ordering nullsLow() {
        return switch (this) {
            case DESCENDING -> DESCENDING_NL;
            case ASCENDING -> ASCENDING_NL;
            default -> this;
        };
    }

    public Ordering nullsFirst() {
        return switch (this) {
            case ASCENDING -> ASCENDING_NL;
            case DESCENDING_NL -> DESCENDING;
            default -> this;
        };
    }

    public Ordering nullsLast() {
        return switch (this) {
            case DESCENDING -> DESCENDING_NL;
            case ASCENDING_NL -> ASCENDING;
            default -> this;
        };
    }

    public Ordering reverse() {
        return switch (this) {
            case ASCENDING -> DESCENDING;
            case DESCENDING -> ASCENDING;
            case ASCENDING_NL -> DESCENDING_NL;
            case DESCENDING_NL -> ASCENDING_NL;
            case UNSPECIFIED -> UNSPECIFIED;
        };
    }
}
