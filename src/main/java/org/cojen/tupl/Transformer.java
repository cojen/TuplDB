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

import java.util.Comparator;

/**
 * Interface which supports filtering and transforming the entries within a {@link
 * View}. For pure filtering, consider implementing a {@link Filter} instead.
 *
 * @author Brian S O'Neill
 * @see View#viewTransformed View.viewTransformed
 */
@FunctionalInterface
public interface Transformer {
    /**
     * Returns true by default, indicating that the transform methods always require a value
     * instance to be provided. When false is returned, values aren't loaded unless explicitly
     * requested. Return null to always use a cursor, in which case the {@link
     * #transformValue(Cursor, byte[]) transformValue} variant which accepts a cursor should be
     * overridden. Otherwise, returning null is equivalent to returning true.
     *
     * @return true if a value must always be passed into the transform methods
     */
    public default Boolean requireValue() {
        return Boolean.TRUE;
    }

    /**
     * Transform or filter out the given value. This method is only called when loading a value
     * from a view.
     *
     * @param value nullable value to transform
     * @param key non-null untransformed key associated with the value
     * @param tkey non-null transformed key associated with the value
     * @return transformed value or null to discard entry
     */
    public abstract byte[] transformValue(byte[] value, byte[] key, byte[] tkey)
        throws IOException;

    /**
     * Transform or filter out the given value. This method is only called when loading from a
     * positioned a cursor. Default implementation always forces the value to be loaded, unless
     * {@link #requireValue requireValue} returns false.
     *
     * @param cursor positioned cursor at the untransformed key and value (not null, might be
     * {@link Cursor#NOT_LOADED NOT_LOADED})
     * @param tkey non-null transformed key associated with the value
     * @return transformed value or null to discard entry
     */
    public default byte[] transformValue(Cursor cursor, byte[] tkey) throws IOException {
        byte[] value = cursor.value();
        if (value == Cursor.NOT_LOADED && requireValue() != Boolean.FALSE) {
            cursor.load();
            value = cursor.value();
        }
        return transformValue(value, cursor.key(), tkey);
    }

    /**
     * Apply an inverse transformation of the given value, if supported. This method is only
     * called when attempting to store the value into the view. Default implementation always
     * throws a {@link ViewConstraintException}.
     *
     * @param tvalue nullable value to transform
     * @param key non-null untransformed key associated with the value
     * @param tkey non-null transformed key associated with the value
     * @return inverse transformed value, or null to delete the value
     * @throws ViewConstraintException if inverse transformation of given value is not supported
     */
    public default byte[] inverseTransformValue(byte[] tvalue, byte[] key, byte[] tkey)
        throws ViewConstraintException, IOException
    {
        throw new ViewConstraintException("Inverse transform isn't supported");
    }

    /**
     * Transform or filter out the given key. This method is only called after positioning a
     * cursor. Default implementation returns the same key.
     *
     * @param cursor positioned cursor at the untransformed key and value (might be null or
     * {@link Cursor#NOT_LOADED NOT_LOADED})
     * @return transformed key or null to discard entry
     */
    public default byte[] transformKey(Cursor cursor) throws IOException {
        return cursor.key();
    }

    /**
     * Apply an inverse transformation of the given key, if supported. This method can be
     * called for load and store operations. Default implementation returns the same key.
     *
     * @param tkey non-null key
     * @return inverse transformed key or null if inverse transformation of given key is not
     * supported
     */
    public default byte[] inverseTransformKey(byte[] tkey) {
        return tkey;
    }

    /**
     * Apply an inverse transformation of the given key, strictly greater than the one given.
     * This method is only called after the regular inverse transformation has been attempted.
     * Default implementation increments the key by the minimum amount and then calls
     * inverseTransformKey.
     *
     * @param tkey non-null key
     * @return inverse transformed key or null if inverse transformation of given key is not
     * supported
     */
    public default byte[] inverseTransformKeyGt(byte[] tkey) {
        tkey = tkey.clone();
        return Utils.increment(tkey, 0, tkey.length) ? inverseTransformKey(tkey) : null;
    }

    /**
     * Apply an inverse transformation of the given key, strictly less than the one given.
     * This method is only called after the regular inverse transformation has been attempted.
     * Default implementation decrements the key by the minimum amount and then calls
     * inverseTransformKey.
     *
     * @param tkey non-null key
     * @return inverse transformed key or null if inverse transformation of given key is not
     * supported
     */
    public default byte[] inverseTransformKeyLt(byte[] tkey) {
        tkey = tkey.clone();
        return Utils.decrement(tkey, 0, tkey.length) ? inverseTransformKey(tkey) : null;
    }

    /**
     * Returns the natural ordering of keys, after they have been transformed. Default
     * implementation returns the same ordering.
     *
     * @param original natural ordering of view before transformation
     */
    public default Ordering transformedOrdering(Ordering original) {
        return original;
    }

    /**
     * Returns the view comparator, after transformation. Default implementation returns the
     * same comparator.
     *
     * @param original comparator of view before transformation
     * @throws IllegalStateException if transformed view is unordered
     */
    public default Comparator<byte[]> transformedComparator(Comparator<byte[]> original) {
        return original;
    }
}
