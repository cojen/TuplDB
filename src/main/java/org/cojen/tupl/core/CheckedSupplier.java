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
        check(findCaller(skip));
        return null;
    }

    /**
     * @throws IllegalCallerException if the caller is in a module that does not have native
     * access enabled
     */
    @Override
    default T get() {
        check(findCaller(1));
        try {
            return doGet();
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    T doGet() throws Throwable;

    /**
     * Finds the caller for performing a permission check. All stack frames in the base module
     * are skipped, because MethodHandles are defined in the base module and can be used to
     * hide the caller activity. In particular, MethodHandles.Lookup.findConstructor allows an
     * object to be constructed without the true caller from being exposed.
     *
     * @param skip typically at least 1
     * @return null if unknown (permission check should fail)
     */
    private static Class<?> findCaller(int skip) {
        // Note that 1 is added to the skip, to skip this method's frame.
        return StackWalker.getInstance
            (EnumSet.of(StackWalker.Option.DROP_METHOD_INFO,
                        StackWalker.Option.RETAIN_CLASS_REFERENCE,
                        StackWalker.Option.SHOW_HIDDEN_FRAMES))
            .walk(s -> s.map(StackWalker.StackFrame::getDeclaringClass)
                  .filter(c -> !"java.base".equals(c.getModule().getName()))
                  .skip(skip + 1).findFirst())
            .orElse(null);
    }

    private static void check(Class<?> caller) {
        Module module = caller == null ? null : caller.getModule();
        if (module == null || !Utils.isNativeAccessEnabled(module)) {
            String message = "Native access isn't enabled";
            if (module != null) {
                message = message + " for " + module;
            }
            throw new IllegalCallerException(message);
        }
    }
}
