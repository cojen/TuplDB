/*
 *  Copyright (C) 2022 Cojen.org
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
 * Defines a row type consisting of a simple binary key-value pair. Calling {@link
 * Index#asTable asTable(Entry.class)} allows an ordinary index to be represented as a table,
 * while still retaining its natural form.
 *
 * @author Brian S O'Neill
 */
@PrimaryKey("key")
public interface Entry extends Comparable<Entry> {
    /**
     * Returns the entry's current key reference. The contents of the array should not be
     * modified, since locks might be held against the key instance itself.
     */
    @Unsigned
    byte[] key();

    /**
     * Assign the entry key reference, transferring ownership. After calling this method the
     * contents of the array should not be modified, since locks might later be acquired
     * against the key instance itself.
     */
    void key(byte[] key);

    /**
     * Returns the entry's current value reference. Although the contents of the array can be
     * safely modified, the change won't be noticed when calling an {@link Table#update update}
     * or {@link Table#merge merge} method.
     */
    @Unsigned
    byte[] value();

    /**
     * Assign the entry value, allowing the change to be noticed by a call to an {@link
     * Table#update update} or {@link Table#merge merge} method.
     */
    void value(byte[] value);
}
