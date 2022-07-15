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

package org.cojen.tupl.rows;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class CodeUtils {
    /**
     * Allocates a new instance if the given variable is null or else blindly casts it to the
     * given type.
     */
    static Variable castOrNew(Variable var, Class<?> type) {
        final var typedVar = var.cast(type);
        MethodMaker mm = var.methodMaker();
        Label notNull = mm.label();
        typedVar.ifNe(null, notNull);
        typedVar.set(mm.new_(type));
        notNull.here();
        return typedVar;
    }

    /**
     * Allocates a new instance if the given variable is null or else casts it to the given
     * type. If the cast would fail, branch to the `other` label.
     */
    static Variable castOrNew(Variable var, Class<?> type, Label other) {
        MethodMaker mm = var.methodMaker();
        final Variable typedVar = mm.var(type);
        Label notNull = mm.label();
        var.ifNe(null, notNull);
        typedVar.set(mm.new_(type));
        Label done = mm.label().goto_();
        notNull.here();
        var.instanceOf(type).ifFalse(other);
        typedVar.set(var.cast(type));
        done.here();
        return typedVar;
    }

    /**
     * Casts the given variable to a RowConsumer, calls the accept method, and then returns the
     * variable from the method being generated. If the cast fails, the given rowClass is used
     * in the thrown ClassCastException.
     */
    static void acceptAsRowConsumerAndReturn(Variable var, Class<?> rowClass,
                                             Object key, Object value)
    {
        MethodMaker mm = var.methodMaker();
        Label tryStart = mm.label().here();
        var consumerVar = var.cast(RowConsumer.class);
        Label tryEnd = mm.label().here();
        consumerVar.invoke("accept", key, value);
        mm.return_(var);
        Variable ex = mm.catch_(tryStart, tryEnd, ClassCastException.class);
        // If this cast succeeds, then the original exception is thrown instead.
        var.cast(rowClass);
        ex.throw_();
    }
}
