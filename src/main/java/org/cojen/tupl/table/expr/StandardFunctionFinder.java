/*
 *  Copyright (C) 2024 Cojen.org
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

package org.cojen.tupl.table.expr;

import java.util.function.Consumer;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.table.Converter;
import org.cojen.tupl.table.SoftCache;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class StandardFunctionFinder extends SoftCache<String, Object, Object>
    implements FunctionFinder
{
    public static final FunctionFinder THE = new StandardFunctionFinder();

    @Override
    public FunctionApplier tryFindFunction(String name,
                                           Type[] argTypes, String[] argNames, Object[] args,
                                           Consumer<String> reason)
    {
        // Note: Only the function name is examined for now. The FunctionApplier.validate
        // method performs additional parameter matching checks.
        return obtain(name, null) instanceof FunctionApplier fa ? fa : null;
    }

    @Override
    protected Object newValue(String name, Object unused) {
        try {
            Class<?> clazz = Class.forName(getClass().getName() + '$' + name);
            return (FunctionApplier) clazz.getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException e) {
            // Negative cache.
            return new Object();
        } catch (Exception e) {
            throw Utils.rethrow(e);
        }
    }

    private static class coalesce extends FunctionApplier.Plain {
        @Override
        public Type validate(Type[] argTypes, String[] argNames, Consumer<String> reason) {
            if (argTypes.length == 0) {
                reason.accept("at least one argument is required");
                return null;
            }

            for (String name : argNames) {
                if (name != null) {
                    reason.accept("unknown parameter: " + name);
                    return null;
                }
            }

            Type type = argTypes[0];

            for (int i=1; i<argTypes.length; i++) {
                type = type.commonType(argTypes[i], -1);
                if (type == null) {
                    reason.accept("no common type");
                    return null;
                }
            }

            for (int i=0; i<argTypes.length; i++) {
                argTypes[i] = type;
            }

            return type;
        }

        @Override
        public void apply(Type retType, Variable retVar, Type[] argTypes, LazyArg[] args) {
            if (retVar.classType().isPrimitive()) {
                // No arguments can be null, so just return the first one.
                retVar.set(args[0].eval(true));
                return;
            }

            MethodMaker mm = retVar.clear().methodMaker();
            Label done = mm.label();

            for (int i=0; i<args.length; i++) {
                LazyArg arg = args[i];

                if (!arg.isConstant()) {
                    var argVar = arg.eval(i == 0);
                    Label next = mm.label();
                    argVar.ifEq(null, next);
                    retVar.set(argVar);
                    done.goto_();
                    next.here();
                } else if (arg.value() != null) {
                    retVar.set(arg.eval(i == 0));
                    // Once a constant non-null value is reached, skip the rest.
                    break;
                }
            }

            done.here();
        }
    }
}
