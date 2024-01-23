/*
 *  Copyright (C) 2023 Cojen.org
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

package org.cojen.tupl.model;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class AnyType extends Type {
    public static final AnyType THE = new AnyType();

    private AnyType() {
        super(Object.class, TYPE_REFERENCE | TYPE_NULLABLE);
    }

    @Override
    public AnyType nullable() {
        return this;
    }

    @Override
    public AnyType commonType(Node node, int op) {
        return this;
    }

    @Override
    public AnyType commonType(Type node, int op) {
        return this;
    }

    @Override
    public int hashCode() {
        return 1872601810;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == THE;
    }

    @Override
    public String toString() {
        return "any";
    }
}
