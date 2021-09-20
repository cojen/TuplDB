/*
 *  Copyright 2021 Cojen.org
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

import java.lang.annotation.*;

/**
 * Annotation which defines the set of columns that uniquely identify a row instance. A row
 * definition must have exactly one {@code @PrimaryKey} annotation.
 *
 * <p>A primary key is itself a special kind of index, and so no additional annotations are
 * required to ensure that searches against the primary key are efficient.
 *
 * @author Brian S O'Neill
 * @see Table
 * @see AlternateKey
 * @see SecondaryIndex
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface PrimaryKey {
    /**
     * The set of column names within primary key, whose ordering affects the natural ordering
     * of the rows within the table. Column names can be optionally prefixed with a '+' or '-'
     * character to indicate ascending or descending order. By default, column order is ascending.
     */
    String[] value();
}

