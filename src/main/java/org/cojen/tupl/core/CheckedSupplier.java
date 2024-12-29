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

package org.cojen.tupl.core;

import java.lang.foreign.MemorySegment;

import java.util.EnumSet;

import java.util.function.Supplier;

/**
 * Checks that the caller is in a module that has native access enabled.
 *
 * @author Brian S. O'Neill
 */
@FunctionalInterface
public interface CheckedSupplier<T> extends Supplier<T> {
    /**
     * Perform a check when the immediate caller is a constructor which already has native
     * access enabled.
     *
     * @param skip number of stack frames to skip, typically 2
     * @return null
     * @throws IllegalCallerException if the caller is in a module that does not have native
     * access enabled
     */
    static Object check(int skip) {
        Class<?> caller = StackWalker.getInstance
            (EnumSet.of(StackWalker.Option.DROP_METHOD_INFO,
                        StackWalker.Option.RETAIN_CLASS_REFERENCE))
            .walk(s -> s.map(StackWalker.StackFrame::getDeclaringClass).skip(skip).findFirst())
            .orElse(null);

        check(caller);

        return null;
    }

    /**
     * @throws IllegalCallerException if the caller is in a module that does not have native
     * access enabled
     */
    @Override
    default T get() {
        Class<?> caller = StackWalker.getInstance
            (EnumSet.of(StackWalker.Option.DROP_METHOD_INFO,
                        StackWalker.Option.RETAIN_CLASS_REFERENCE)).getCallerClass();

        check(caller);

        try {
            return doGet();
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    T doGet() throws Throwable;

    private static void check(Class<?> caller) {
        Module module = caller == null ? null : caller.getModule();
        if (module == null || !module.isNativeAccessEnabled()) {
            if (module != null) {
                // Calling a restricted API can potentilly enable access for the module,
                // although this won't work in a future Java release.
                MemorySegment.NULL.reinterpret(0);
                if (module.isNativeAccessEnabled()) {
                    return;
                }
            }
            throw new IllegalCallerException("Native access isn't enabled for " + module);
        }
    }
}
