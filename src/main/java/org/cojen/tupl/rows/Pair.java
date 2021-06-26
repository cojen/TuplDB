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

package org.cojen.tupl.rows;

import java.util.Objects;

/**
 * Composite cache key.
 *
 * @author Brian S O'Neill
 */
class Pair {
    private final Object a, b;

    Pair(Object a, Object b) {
        this.a = a;
        this.b = b;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof Pair) {
            var other = (Pair) obj;
            return Objects.equals(a, other.a) && Objects.equals(b, other.b);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(a) * 31 + Objects.hashCode(b);
    }
}
