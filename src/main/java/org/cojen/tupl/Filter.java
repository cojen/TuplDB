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
 * View transformer which filters out entries which don't belong. Implementations only need to
 * implement the {@link #isAllowed isAllowed} method.
 *
 * @author Brian S O'Neill
 * @see View#viewTransformed View.viewTransformed
 */
@FunctionalInterface
public interface Filter extends Transformer {
    /**
     * Return true if the given key and value are not to be filtered out.
     */
    public abstract boolean isAllowed(byte[] key, byte[] value) throws IOException;

    /**
     * Calls the {@link #isAllowed isAllowed} method.
     */
    @Override
    public default byte[] transformValue(byte[] value, byte[] key, byte[] tkey)
        throws IOException
    {
        return isAllowed(key, value) ? value : null;
    }

    /**
     * Calls the {@link #isAllowed isAllowed} method.
     */
    @Override
    public default byte[] inverseTransformValue(byte[] tvalue, byte[] key, byte[] tkey)
        throws IOException, ViewConstraintException
    {
        if (!isAllowed(key, tvalue)) {
            throw new ViewConstraintException("Filtered out");
        }
        return tvalue;
    }
}
