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

/**
 * 
 *
 * @author Brian S. O'Neill
 */
// Must be public.
public final class RemapUtils {
    /**
     * Called by generated MappedQueryExpr remap code for an 'and' filter.
     *
     * @param left Boolean or RuntimeException
     * @param right Boolean or RuntimeException
     * @return Boolean or RuntimeException
     */
    public static Object checkAnd(Object left, Object right) {
        // If the left or right result is false, then return false, potentially supressing a
        // RuntimeException from the other result. If neither side is a RuntimeException, then
        // return the appropriate 'and' result.

        if (left instanceof Boolean leftBool) {
            if (leftBool == Boolean.FALSE) {
                return leftBool;
            }
            if (right instanceof Boolean rightBool) {
                return rightBool;
            }
            throw (RuntimeException) right;
        }

        if (right instanceof Boolean rightBool && rightBool == Boolean.FALSE) {
            return rightBool;
        }

        throw (RuntimeException) left;
    }

    /**
     * Called by generated MappedQueryExpr remap code for an 'or' filter.
     *
     * @param left Boolean or RuntimeException
     * @param right Boolean or RuntimeException
     * @return Boolean or RuntimeException
     */
    public static Object checkOr(Object left, Object right) {
        // If the left or right result is true, then return true, potentially supressing a
        // RuntimeException from the other result. If neither side is a RuntimeException, then
        // return the appropriate 'or' result.

        if (left instanceof Boolean leftBool) {
            if (leftBool == Boolean.TRUE) {
                return leftBool;
            }
            if (right instanceof Boolean rightBool) {
                return rightBool;
            }
            throw (RuntimeException) right;
        }

        if (right instanceof Boolean rightBool && rightBool == Boolean.TRUE) {
            return rightBool;
        }

        throw (RuntimeException) left;
    }

    /**
     * Called by generated MappedQueryExpr remap code.
     *
     * @param originalEx optional
     * @param result Boolean or RuntimeException
     * @return boolean is result is Boolean
     * @throws RuntimeException if result is RuntimeException; throws originalEx if not null
     */
    public static boolean checkFinal(RuntimeException originalEx, Object result) {
        if (result instanceof Boolean b) {
            return b;
        }
        if (originalEx != null) {
            throw originalEx;
        }
        throw (RuntimeException) result;
    }
}
