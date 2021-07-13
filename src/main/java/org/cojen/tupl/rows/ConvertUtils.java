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

import org.cojen.tupl.filter.ColumnFilter;

import static org.cojen.tupl.rows.ColumnInfo.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ConvertUtils {
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

    /**
     * Finds a common type which two columns can be converted to without loss. The name of the
     * returned ColumnInfo is undefined (it might be null).
     *
     * @param op defined in ColumnFilter
     * @return null if a common type cannot be inferred or is ambiguous
     */
    public static ColumnInfo commonType(ColumnInfo aInfo, ColumnInfo bInfo, int op) {
        int aTypeCode = aInfo.typeCode & ~TYPE_DESCENDING;
        int bTypeCode = bInfo.typeCode & ~TYPE_DESCENDING;

        if (aTypeCode == bTypeCode) {
            return aInfo;
        }

        if (isNullable(aTypeCode) || isNullable(bTypeCode)) {
            // Common type shall be nullable.
            aTypeCode |= TYPE_NULLABLE;
            bTypeCode |= TYPE_NULLABLE;
        }

        if (isArray(aTypeCode) || isArray(bTypeCode)) {
            if (isArray(aTypeCode)) {
                aInfo = aInfo.nonArray();
            }
            if (isArray(bTypeCode)) {
                bInfo = bInfo.nonArray();
            }
            ColumnInfo cInfo = commonType(aInfo, bInfo, op);
            if (cInfo != null) {
                cInfo = cInfo.asArray(ColumnInfo.isNullable(aTypeCode));
            }
            return cInfo;
        }

        // Order aTypeCode to be less than bTypeCode to reduce the number of permutations.
        if (bTypeCode < aTypeCode) {
            int tmp = aTypeCode;
            aTypeCode = bTypeCode;
            bTypeCode = tmp;
        }

        int aPlainCode = plainTypeCode(aTypeCode);
        int bPlainCode = plainTypeCode(bTypeCode);

        final int cPlainCode;

        select: if (aPlainCode == bPlainCode) {
            cPlainCode = aPlainCode;
        } else {
            switch (aPlainCode) {
            case TYPE_BOOLEAN:
                if (!ColumnFilter.isExact(op)) {
                    return null;
                }
                // Note: Boolean to number is always 0 or 1. Boolean to char is 'f' or 't', and
                // boolean to string is "false" or "true".
                switch (bPlainCode) {
                case TYPE_UBYTE:
                case TYPE_USHORT:
                case TYPE_UINT:
                case TYPE_ULONG:
                case TYPE_BYTE:
                case TYPE_SHORT:
                case TYPE_INT:
                case TYPE_LONG:
                case TYPE_FLOAT:
                case TYPE_DOUBLE:
                case TYPE_CHAR:
                case TYPE_UTF8:
                case TYPE_BIG_INTEGER:
                case TYPE_BIG_DECIMAL: cPlainCode = bPlainCode; break select;
                default: return null;
                }

            case TYPE_UBYTE:
                switch (bPlainCode) {
                case TYPE_USHORT: cPlainCode = TYPE_USHORT; break select;
                case TYPE_UINT: cPlainCode = TYPE_UINT; break select;
                case TYPE_ULONG: cPlainCode = TYPE_ULONG; break select;
                case TYPE_BYTE:
                case TYPE_SHORT: cPlainCode = TYPE_SHORT; break select;
                case TYPE_INT: cPlainCode = TYPE_INT; break select;
                case TYPE_LONG: cPlainCode = TYPE_LONG; break select;
                case TYPE_FLOAT: cPlainCode = TYPE_FLOAT; break select;
                case TYPE_DOUBLE: cPlainCode = TYPE_DOUBLE; break select;
                case TYPE_CHAR: cPlainCode = TYPE_USHORT; break select;
                case TYPE_UTF8: cPlainCode = -1; break select;
                case TYPE_BIG_INTEGER: cPlainCode = TYPE_BIG_INTEGER; break select;
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_USHORT:
                switch (bPlainCode) {
                case TYPE_UINT: cPlainCode = TYPE_UINT; break select;
                case TYPE_ULONG: cPlainCode = TYPE_ULONG; break select;
                case TYPE_BYTE:
                case TYPE_SHORT:
                case TYPE_INT: cPlainCode = TYPE_INT; break select;
                case TYPE_LONG: cPlainCode = TYPE_LONG; break select;
                case TYPE_FLOAT: cPlainCode = TYPE_FLOAT; break select;
                case TYPE_DOUBLE: cPlainCode = TYPE_DOUBLE; break select;
                case TYPE_CHAR: cPlainCode = TYPE_USHORT; break select;
                case TYPE_UTF8: cPlainCode = -1; break select;
                case TYPE_BIG_INTEGER: cPlainCode = TYPE_BIG_INTEGER; break select;
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_UINT:
                switch (bPlainCode) {
                case TYPE_ULONG: cPlainCode = TYPE_ULONG; break select;
                case TYPE_BYTE:
                case TYPE_SHORT:
                case TYPE_INT:
                case TYPE_LONG: cPlainCode = TYPE_LONG; break select;
                case TYPE_FLOAT: cPlainCode = TYPE_DOUBLE; break select;
                case TYPE_DOUBLE: cPlainCode = TYPE_BIG_DECIMAL; break select;
                case TYPE_CHAR: cPlainCode = TYPE_UINT; break select;
                case TYPE_UTF8: cPlainCode = -1; break select;
                case TYPE_BIG_INTEGER: cPlainCode = TYPE_BIG_INTEGER; break select;
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_ULONG:
                switch (bPlainCode) {
                case TYPE_BYTE:
                case TYPE_SHORT:
                case TYPE_INT:
                case TYPE_LONG: cPlainCode = TYPE_BIG_INTEGER; break select;
                case TYPE_FLOAT:
                case TYPE_DOUBLE: cPlainCode = TYPE_BIG_DECIMAL; break select;
                case TYPE_CHAR: cPlainCode = TYPE_ULONG; break select;
                case TYPE_UTF8: cPlainCode = -1; break select;
                case TYPE_BIG_INTEGER: cPlainCode = TYPE_BIG_INTEGER; break select;
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_BYTE:
                switch (bPlainCode) {
                case TYPE_SHORT: cPlainCode = TYPE_SHORT; break select;
                case TYPE_INT: cPlainCode = TYPE_INT; break select;
                case TYPE_LONG: cPlainCode = TYPE_LONG; break select;
                case TYPE_FLOAT: cPlainCode = TYPE_FLOAT; break select;
                case TYPE_DOUBLE: cPlainCode = TYPE_DOUBLE; break select;
                case TYPE_CHAR: cPlainCode = TYPE_INT; break select;
                case TYPE_UTF8: cPlainCode = -1; break select;
                case TYPE_BIG_INTEGER: cPlainCode = TYPE_BIG_INTEGER; break select;
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_SHORT:
                switch (bPlainCode) {
                case TYPE_INT: cPlainCode = TYPE_INT; break select;
                case TYPE_LONG: cPlainCode = TYPE_LONG; break select;
                case TYPE_FLOAT: cPlainCode = TYPE_FLOAT; break select;
                case TYPE_DOUBLE: cPlainCode = TYPE_DOUBLE; break select;
                case TYPE_CHAR: cPlainCode = TYPE_INT; break select;
                case TYPE_UTF8: cPlainCode = -1; break select;
                case TYPE_BIG_INTEGER: cPlainCode = TYPE_BIG_INTEGER; break select;
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_INT:
                switch (bPlainCode) {
                case TYPE_LONG: cPlainCode = TYPE_LONG; break select;
                case TYPE_FLOAT:
                case TYPE_DOUBLE: cPlainCode = TYPE_DOUBLE; break select;
                case TYPE_CHAR: cPlainCode = TYPE_INT; break select;
                case TYPE_UTF8: cPlainCode = -1; break select;
                case TYPE_BIG_INTEGER: cPlainCode = TYPE_BIG_INTEGER; break select;
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_LONG:
                switch (bPlainCode) {
                case TYPE_FLOAT:
                case TYPE_DOUBLE: cPlainCode = TYPE_BIG_DECIMAL; break select;
                case TYPE_CHAR: cPlainCode = TYPE_LONG; break select;
                case TYPE_UTF8: cPlainCode = -1; break select;
                case TYPE_BIG_INTEGER: cPlainCode = TYPE_BIG_INTEGER; break select;
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_FLOAT:
                switch (bPlainCode) {
                case TYPE_DOUBLE: cPlainCode = TYPE_DOUBLE; break select;
                case TYPE_CHAR: cPlainCode = TYPE_FLOAT; break select;
                case TYPE_UTF8: cPlainCode = -1; break select;
                case TYPE_BIG_INTEGER:
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_DOUBLE:
                switch (bPlainCode) {
                case TYPE_CHAR: cPlainCode = TYPE_DOUBLE; break select;
                case TYPE_UTF8: cPlainCode = -1; break select;
                case TYPE_BIG_INTEGER:
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_CHAR:
                switch (bPlainCode) {
                case TYPE_UTF8: cPlainCode = TYPE_UTF8; break select;
                case TYPE_BIG_INTEGER: cPlainCode = TYPE_BIG_INTEGER; break select;
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_UTF8:
                switch (bPlainCode) {
                case TYPE_BIG_INTEGER:
                case TYPE_BIG_DECIMAL: cPlainCode = -1; break select;
                default: return null;
                }

            case TYPE_BIG_INTEGER:
                switch (bPlainCode) {
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_BIG_DECIMAL:
                switch (bPlainCode) {
                default: return null;
                }

            default: return null;
            }
        }

        int cTypeCode = cPlainCode;

        if (cTypeCode == -1) {
            // Mixed numerical and string comparison is ambiguous if not an exact comparison.
            // What does 5 < "a" mean? Is this a numerical or lexicographical comparison?
            if (!ColumnFilter.isExact(op)) {
                return null;
            }
            cTypeCode = TYPE_UTF8;
        }

        cTypeCode |= (aTypeCode & TYPE_NULLABLE);

        var cInfo = new ColumnInfo();

        cInfo.typeCode = cTypeCode;
        cInfo.assignType();

        return cInfo;
    }
}
