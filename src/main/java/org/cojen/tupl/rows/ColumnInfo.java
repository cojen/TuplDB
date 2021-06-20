/*
 *  Copyright 2021 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.rows;

import java.lang.reflect.Method;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ColumnInfo implements Cloneable {

    /*
      Type code format:

      0b...00000: u1 (boolean)
      0b...00001: u2
      0b...00010: u4
      0b...00011: u8
      0b...00100: u16
      0b...00101: u32
      0b...00110: u64
      0b...00111: u128
      0b...01000: i1
      0b...01001: i2
      0b...01010: i4
      0b...01011: i8  (byte)
      0b...01100: i16 (short)
      0b...01101: i32 (int)
      0b...01110: i64 (long)
      0b...01111: i128

      0b...10000: float16
      0b...10001: float32 (float)
      0b...10010: float64 (double)
      0b...10011: float128

      0b...10100: char16 (char)
      0b...10101: char32
      0b...10110: unused
      0b...10111: char8

      0b...11000: utf8 string
      0b...11001: unused (ISO-LATIN1 string?)
      0b...11010: unused (UTF-16 string?)
      0b...11011: unused (UTF-32 string?)

      0b...11100: big integer
      0b...11101: big decimal
      0b...11110: document
      0b...11111: unused

      Modifiers:

      0b000.....: plain
      0b1.......: descending order
      0b.1......: nullable
      0b..1.....: array
    */

    public static final int
        TYPE_BOOLEAN     = 0b000_00000,
        TYPE_UBYTE       = 0b000_00011,
        TYPE_USHORT      = 0b000_00100,
        TYPE_UINT        = 0b000_00101,
        TYPE_ULONG       = 0b000_00110,
        TYPE_BYTE        = 0b000_01011,
        TYPE_SHORT       = 0b000_01100,
        TYPE_INT         = 0b000_01101,
        TYPE_LONG        = 0b000_01110,
        TYPE_FLOAT       = 0b000_10001,
        TYPE_DOUBLE      = 0b000_10010,
        TYPE_CHAR        = 0b000_10100,
        TYPE_UTF8        = 0b000_11000,
        TYPE_BIG_INTEGER = 0b000_11100,
        TYPE_BIG_DECIMAL = 0b000_11101;

    static final int
        TYPE_DESCENDING  = 0b100_00000,
        TYPE_NULLABLE    = 0b010_00000,
        TYPE_ARRAY       = 0b001_00000;

    String name;
    Class<?> type;
    int typeCode;

    Method accessor, mutator;

    public String name() {
        return name;
    }

    public int plainTypeCode() {
        return plainTypeCode(typeCode);
    }

    static int plainTypeCode(int typeCode) {
        return typeCode & 0b000_11111;
    }

    boolean isUnsigned() {
        return isUnsigned(typeCode);
    }

    static boolean isUnsigned(int typeCode) {
        return plainTypeCode(typeCode) < 0b000_01000;
    }

    boolean isUnsignedInteger() {
        return isUnsignedInteger(typeCode);
    }

    static boolean isUnsignedInteger(int typeCode) {
        return isUnsigned(typeCode) && plainTypeCode(typeCode) != TYPE_BOOLEAN;
    }

    boolean isDescending() {
        return isDescending(typeCode);
    }

    static boolean isDescending(int typeCode) {
        return (typeCode & TYPE_DESCENDING) != 0;
    }

    boolean isNullable() {
        return isNullable(typeCode);
    }

    static boolean isNullable(int typeCode) {
        return (typeCode & TYPE_NULLABLE) != 0;
    }

    boolean isArray() {
        return isArray(typeCode);
    }

    static boolean isArray(int typeCode) {
        return (typeCode & TYPE_ARRAY) != 0;
    }

    boolean isPrimitive() {
        return isPrimitive(typeCode);
    }

    static boolean isPrimitive(int typeCode) {
        return (typeCode & ~TYPE_DESCENDING) < 0b000_11000;
    }

    /**
     * Assigns the type by examining the typeCode.
     */
    void assignType() {
        final Class<?> plainType = isNullable() ? boxedType() : unboxedType();
        this.type = isArray() ? plainType.arrayType() : plainType;
    }

    /**
     * Return the primitive boxed type or the regular type if not primitive.
     */
    Class<?> boxedType() {
        switch (plainTypeCode()) {
        case TYPE_BOOLEAN:                 return Boolean.class;
        case TYPE_BYTE:  case TYPE_UBYTE:  return Byte.class;
        case TYPE_SHORT: case TYPE_USHORT: return Short.class;
        case TYPE_INT:   case TYPE_UINT:   return Integer.class;
        case TYPE_LONG:  case TYPE_ULONG:  return Long.class;
        case TYPE_FLOAT:                   return Float.class;
        case TYPE_DOUBLE:                  return Double.class;
        case TYPE_UTF8:                    return String.class;
        case TYPE_BIG_INTEGER:             return BigInteger.class;
        case TYPE_BIG_DECIMAL:             return BigDecimal.class;
        case TYPE_CHAR:                    return Character.class;
        default:                           return Object.class;
        }
    }

    /**
     * Return the primitive unboxed type or the regular type if not primitive.
     */
    Class<?> unboxedType() {
        switch (plainTypeCode()) {
        case TYPE_BOOLEAN:                 return boolean.class;
        case TYPE_BYTE:  case TYPE_UBYTE:  return byte.class;
        case TYPE_SHORT: case TYPE_USHORT: return short.class;
        case TYPE_INT:   case TYPE_UINT:   return int.class;
        case TYPE_LONG:  case TYPE_ULONG:  return long.class;
        case TYPE_FLOAT:                   return float.class;
        case TYPE_DOUBLE:                  return double.class;
        case TYPE_UTF8:                    return String.class;
        case TYPE_BIG_INTEGER:             return BigInteger.class;
        case TYPE_BIG_DECIMAL:             return BigDecimal.class;
        case TYPE_CHAR:                    return char.class;
        default:                           return Object.class;
        }
    }

    /**
     * Returns true if the given type is the same as this one or can be primitively widened.
     */
    boolean isAssignableFrom(ColumnInfo other) {
        if (this.typeCode == other.typeCode) {
            return true;
        }

        if (other.isArray() && !this.isArray()) {
            return false;
        }

        if (other.isNullable() && !this.isNullable()) {
            return false;
        }

        return isAssignableFrom(other.plainTypeCode());
    }

    boolean isAssignableFrom(int otherPlainTypeCode) {
        int thisPlainTypeCode = this.plainTypeCode();

        if (thisPlainTypeCode == otherPlainTypeCode) {
            return true;
        }

        switch (thisPlainTypeCode) {
        case TYPE_SHORT:
            return otherPlainTypeCode == TYPE_BYTE;
        case TYPE_INT: case TYPE_FLOAT:
            switch (otherPlainTypeCode) {
            case TYPE_BYTE: case TYPE_SHORT: case TYPE_CHAR:
                return true;
            default:
                return false;
            }
        case TYPE_LONG:
            switch (otherPlainTypeCode) {
            case TYPE_BYTE: case TYPE_SHORT: case TYPE_CHAR: case TYPE_INT:
                return true;
            default:
                return false;
            }
        case TYPE_DOUBLE:
            switch (otherPlainTypeCode) {
            case TYPE_BYTE: case TYPE_SHORT: case TYPE_CHAR: case TYPE_INT: case TYPE_FLOAT:
                return true;
            default:
                return false;
            }
        default:
            return false;
        }
    }

    /**
     * Returns this instance if already non-nullable, else a copy is returned.
     */
    ColumnInfo asNonNullable() {
        if (!isNullable()) {
            return this;
        } else {
            ColumnInfo info = copy();
            info.typeCode &= ~TYPE_NULLABLE;
            return info;
        }
    }

    ColumnInfo copy() {
        try {
            return (ColumnInfo) clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    @Override
    public int hashCode() {
        return name.hashCode() * 31 + typeCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof ColumnInfo) {
            var other = (ColumnInfo) obj;
            return typeCode == other.typeCode && name.equals(other.name);
        }
        return false;
    }

    @Override
    public String toString() {
        return (typeCode != -1 && isDescending()) ? ('-' + name) : name;
    }
}
