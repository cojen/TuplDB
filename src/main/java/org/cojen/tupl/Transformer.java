/*
 *  Copyright 2014-2015 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl;

import java.io.IOException;

/**
 * Interface which supports filtering and transforming the entries within a {@link
 * View}. For pure filtering, consider implementing a {@link Filter} instead.
 *
 * @author Brian S O'Neill
 * @see View#viewTransformed View.viewTransformed
 */
public interface Transformer {
    /**
     * Returns true by default, indicating that the transform methods always require a value
     * instance to be provided.
     *
     * @return true if a value must always be passed into the transform methods
     */
    public default boolean requireValue() {
        return true;
    }

    /**
     * Transform or filter out the given value. This method is only called after loading a
     * value from a view.
     *
     * @param value nullable value to transform
     * @param key non-null untransformed key associated with the value
     * @param tkey non-null transformed key associated with the value
     * @return transformed value or null to discard entry
     */
    public abstract byte[] transformValue(byte[] value, byte[] key, byte[] tkey)
        throws IOException;

    /**
     * Apply an inverse transformation of the given value, if supported. This method is only
     * called when attempting to store the value into the view.
     *
     * @param tvalue nullable value to transform
     * @param key non-null untransformed key associated with the value
     * @param tkey non-null transformed key associated with the value
     * @return inverse transformed value, or null to delete the value
     * @throws ViewConstraintException if inverse transformation of given value is not supported
     */
    public abstract byte[] inverseTransformValue(byte[] tvalue, byte[] key, byte[] tkey)
        throws ViewConstraintException, IOException;

    /**
     * Transform or filter out the given key. This method is only called after loading a value
     * from a view. Default implementation returns the same key.
     *
     * @param key non-null key to transform
     * @param value nullable value associated with the key
     * @return transformed key or null to discard entry
     */
    public default byte[] transformKey(byte[] key, byte[] value) throws IOException {
        return key;
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
}
