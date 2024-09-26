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

import java.util.List;
import java.util.Map;

import java.util.function.Consumer;

/**
 * @author Brian S. O'Neill
 */
public interface FunctionFinder {
    /**
     * Tries to find a function by the given name and arguments.
     *
     * @param name function name
     * @param args non-null array of unnamed argument types
     * @param namedArgs non-null map of named argument types
     * @param reason if the function isn't found, optionally provide a reason
     * @return null if not found
     */
    FunctionApplier tryFindFunction(String name,
                                    List<Expr> args, Map<String, Expr> namedArgs,
                                    Consumer<String> reason);
}
