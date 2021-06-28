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

import java.util.function.Function;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ConvertUtils {
    /**
     * @param toType must be an array type
     * @param lengthVar length of array to create
     * @param elementConverter given an array index, return a converted element
     * @return toType result array
     */
    static Variable convertArray(MethodMaker mm, Class toType, Variable lengthVar,
                                 Function<Variable, Variable> elementConverter)
    {
        var toArrayVar = mm.new_(toType, lengthVar);
        var ixVar = mm.var(int.class).set(0);
        Label start = mm.label().here();
        Label end = mm.label();
        ixVar.ifGe(lengthVar, end);
        toArrayVar.aset(ixVar, elementConverter.apply(ixVar));
        ixVar.inc(1);
        mm.goto_(start);
        end.here();
        return toArrayVar;
    }
}
