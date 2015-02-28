/*
 *  Copyright 2014 Brian S O'Neill
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

/**
 * View transformer which filters out entries which don't belong. Subclasses only need to
 * implement the {@link #isAllowed isAllowed} method.
 *
 * @author Brian S O'Neill
 * @see View#viewTransformed View.viewTransformed
 */
public abstract class Filter extends Transformer {
    /**
     * Return true if the given key and value are not to be filtered out.
     */
    public abstract boolean isAllowed(byte[] key, byte[] value);

    /**
     * Calls the {@link #isAllowed isAllowed} method.
     */
    @Override
    public final byte[] transformValue(byte[] value, byte[] key, byte[] tkey) {
        return isAllowed(key, value) ? value : null;
    }

    /**
     * Calls the {@link #isAllowed isAllowed} method.
     */
    @Override
    public final byte[] inverseTransformValue(byte[] tvalue, byte[] key, byte[] tkey)
        throws ViewConstraintException
    {
        if (!isAllowed(key, tvalue)) {
            throw new ViewConstraintException("Filtered out");
        }
        return tvalue;
    }
}
