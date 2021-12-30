/*
 *  Copyright (C) 2021 Cojen.org
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

import java.lang.annotation.*;

/**
 * Annotation which can be applied to the last column of a primary key, indicating that it
 * should be automatically generated when not explicitly provided. The column type can only be
 * a primitive int or long, and they are assigned randomly and sequentially generated
 * values. The effective range is clamped to be within the bounds of the column type, and zero
 * is never generated.
 *
 * @author Brian S O'Neill
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Automatic {
    /**
     * The minimum value to generate, inclusive. If the column type is an int, the actual
     * minimum won't be lower than the minimum int value. The minimum value can be negative,
     * although zero will never be generated. If the column type is unsigned, negative values
     * wraparound to the high positive range.
     */
    long min() default 1;

    /**
     * The maximum value to generate, inclusive. If the column type is an int, the actual
     * maximum won't be higher than the maximum int value.
     */
    long max() default Long.MAX_VALUE;
}
