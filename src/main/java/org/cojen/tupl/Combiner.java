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

import java.io.IOException;

/**
 * Represents an operation which combines two values that map to the same key.
 *
 * @author Brian S O'Neill
 * @see View#viewUnion viewUnion
 * @see View#viewIntersection viewIntersection
 * @see View#viewDifference viewDifference
 */
@FunctionalInterface
public interface Combiner {
    /**
     * Returns a Combiner that chooses the first value and discards the second value.
     */
    public static Combiner first() {
        return Combiners.FIRST;
    }

    /**
     * Returns a Combiner that chooses the second value and discards the first value.
     */
    public static Combiner second() {
        return Combiners.SECOND;
    }

    /**
     * Returns a Combiner that discards both values (always returns null). When used with a
     * {@link View#viewUnion union}, this causes it to compute the <i>symmetric set difference</i>.
     */
    public static Combiner discard() {
        return (k, first, second) -> null;
    }

    /**
     * Return a combined value derived from the given key and value pair. Null can be returned
     * to completely discard both values.
     *
     * @param key non-null associated key
     * @param first non-null first value in the pair
     * @param second non-null second value in the pair
     * @return combined value or null to discard both values
     */
    public byte[] combine(byte[] key, byte[] first, byte[] second) throws IOException;

    /**
     * Separates a combined result, for use when storing a value into a view which uses this
     * combiner. Throws ViewConstraintException by default.
     *
     * @param key non-null associated key
     * @param value non-null combined value
     * @return first and second value, neither of which can be null
     */
    public default byte[][] separate(byte[] key, byte[] value) throws IOException {
        throw new ViewConstraintException();
    }
}
