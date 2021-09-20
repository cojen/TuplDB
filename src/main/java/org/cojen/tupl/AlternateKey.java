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
 * Annotation which defines an alternate set of columns that uniquely identify a row instance.
 * Row definitions aren't required to have any {@code @AlternateKey} annotations.
 *
 * <p>An alternate key is essentially the same as a {@link SecondaryIndex secondary index},
 * except with a uniqueness constraint applied. Attempting to insert a row with a conflicting
 * alternate key causes a {@link UniqueConstraintException} to be thrown.
 *
 * @author Brian S O'Neill
 * @see Table
 * @see SecondaryIndex
 * @see PrimaryKey
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Repeatable(AlternateKey.Set.class)
public @interface AlternateKey {
    /**
     * The set of column names within alternate key, whose ordering affects the natural
     * ordering of the rows within the alternate key index. Column names can be optionally
     * prefixed with a '+' or '-' character to indicate ascending or descending order. By
     * default, column order is ascending.
     */
    String[] value();

    /**
     * @hidden
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    public @interface Set {
        AlternateKey[] value();
    }
}
