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

package org.cojen.tupl;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Optional interface that rows can extend for supporting reflection-style access to columns.
 *
 * @author Brian S. O'Neill
 * @see Table
 */
public interface Row {
    /**
     * Returns the total number of colums in the row.
     *
     * @return total number of columns in the row
     */
    int columnCount();

    /**
     * Returns the 1-based column number for the given column name.
     *
     * @param name column name
     * @return 1-based column number
     * @throws IllegalArgumentException if the given column name is unknown
     */
    int columnNumber(String name);

    /**
     * Returns the 1-based column number for the given column name, or else returns 0 if the
     * column name is unknown.
     *
     * @param name column name
     * @return 1-based column number, or 0 if unknown
     */
    int findColumnNumber(String name);

    /**
     * Returns the column name for the given 1-based column number.
     *
     * @param number 1-based column number
     * @return column name
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     */
    String columnName(int number);

    /**
     * Returns the method name for the given 1-based column number. The method name can differ
     * from the column name if it needed to be renamed to something legal, or if an explicit
     * name override was specified.
     *
     * @param number 1-based column number
     * @return column method name
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     */
    default String columnMethodName(int number) {
        return columnName(number);
    }

    /**
     * Returns the method name for the given column name. The method name can differ from the
     * column name if it needed to be renamed to something legal, or if an explicit name
     * override was specified.
     *
     * @param name column name
     * @return column method name
     * @throws IllegalArgumentException if the given column name is unknown
     */
    default String columnMethodName(String name) {
        return columnMethodName(columnNumber(name));
    }

    /**
     * Returns the column type for the given 1-based column number.
     *
     * @param number 1-based column number
     * @return column type
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     */
    Class<?> columnType(int number);

    /**
     * Returns the column type for the given column name.
     *
     * @param name column name
     * @return column type
     * @throws IllegalArgumentException if the given column name is unknown
     */
    default Class<?> columnType(String name) {
        return columnType(columnNumber(name));
    }

    /**
     * Returns the value for the given 1-based column number.
     *
     * @param number 1-based column number
     * @return the column value, possibly boxed
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     */
    Object get(int number);

    /**
     * Returns the value for the given column name.
     *
     * @param name column name
     * @return the column value, possibly boxed
     * @throws IllegalArgumentException if the given column name is unknown
     */
    default Object get(String name) {
        return get(columnNumber(name));
    }

    /**
     * Returns a boolean value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @return the column value as a boolean
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    boolean get_boolean(int number);

    /**
     * Returns a boolean value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a boolean
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default boolean get_boolean(String name) {
        return get_boolean(columnNumber(name));
    }

    /**
     * Returns a boolean array value for the given 1-based column number, performing a
     * conversion if necessary.
     *
     * @param number 1-based column number
     * @return the column value as a boolean array
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    boolean[] get_boolean_array(int number);

    /**
     * Returns a boolean array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a boolean array
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default boolean[] get_boolean_array(String name) {
        return get_boolean_array(columnNumber(name));
    }

    /**
     * Returns a byte value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @return the column value as a byte
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    byte get_byte(int number);

    /**
     * Returns a byte value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a byte
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default byte get_byte(String name) {
        return get_byte(columnNumber(name));
    }

    /**
     * Returns a byte array value for the given 1-based column number, performing a conversion
     * if necessary.
     *
     * @param number 1-based column number
     * @return the column value as a byte array
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    byte[] get_byte_array(int number);

    /**
     * Returns a byte array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a byte array
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default byte[] get_byte_array(String name) {
        return get_byte_array(columnNumber(name));
    }

    /**
     * Returns a short value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @return the column value as a short
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    short get_short(int number);

    /**
     * Returns a short value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a short
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default short get_short(String name) {
        return get_short(columnNumber(name));
    }

    /**
     * Returns a short array value for the given 1-based column number, performing a conversion
     * if necessary.
     *
     * @param number 1-based column number
     * @return the column value as a short array
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    short[] get_short_array(int number);

    /**
     * Returns a short array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a short array
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default short[] get_short_array(String name) {
        return get_short_array(columnNumber(name));
    }

    /**
     * Returns a char value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @return the column value as a char
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    char get_char(int number);

    /**
     * Returns a char value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a char
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default char get_char(String name) {
        return get_char(columnNumber(name));
    }

    /**
     * Returns a char array value for the given 1-based column number, performing a conversion
     * if necessary.
     *
     * @param number 1-based column number
     * @return the column value as a char array
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    char[] get_char_array(int number);

    /**
     * Returns a char array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a char array
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default char[] get_char_array(String name) {
        return get_char_array(columnNumber(name));
    }

    /**
     * Returns an int value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @return the column value as an int
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    int get_int(int number);

    /**
     * Returns an int value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as an int
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default int get_int(String name) {
        return get_int(columnNumber(name));
    }

    /**
     * Returns an int array value for the given 1-based column number, performing a conversion
     * if necessary.
     *
     * @param number 1-based column number
     * @return the column value as an int array
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    int[] get_int_array(int number);

    /**
     * Returns an int array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as an int array
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default int[] get_int_array(String name) {
        return get_int_array(columnNumber(name));
    }

    /**
     * Returns a long value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @return the column value as a long
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    long get_long(int number);

    /**
     * Returns a long value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a long
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default long get_long(String name) {
        return get_long(columnNumber(name));
    }

    /**
     * Returns a long array value for the given 1-based column number, performing a conversion
     * if necessary.
     *
     * @param number 1-based column number
     * @return the column value as a long array
     * @throws ConversionException
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     */
    long[] get_long_array(int number);

    /**
     * Returns a long array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a long array
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default long[] get_long_array(String name) {
        return get_long_array(columnNumber(name));
    }

    /**
     * Returns a float value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @return the column value as a float
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    float get_float(int number);

    /**
     * Returns a float value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a float
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default float get_float(String name) {
        return get_float(columnNumber(name));
    }

    /**
     * Returns a float array value for the given 1-based column number, performing a conversion
     * if necessary.
     *
     * @param number 1-based column number
     * @return the column value as a float array
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    float[] get_float_array(int number);

    /**
     * Returns a float array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a float array
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default float[] get_float_array(String name) {
        return get_float_array(columnNumber(name));
    }

    /**
     * Returns a double value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @return the column value as a double array
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    double get_double(int number);

    /**
     * Returns a double value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a double
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default double get_double(String name) {
        return get_double(columnNumber(name));
    }

    /**
     * Returns a double array value for the given 1-based column number, performing a
     * conversion if necessary.
     *
     * @param number 1-based column number
     * @return the column value as a double array
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    double[] get_double_array(int number);

    /**
     * Returns a double array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a double array
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default double[] get_double_array(String name) {
        return get_double_array(columnNumber(name));
    }

    /**
     * Returns a Boolean value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @return the column value as a Boolean
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    Boolean getBoolean(int number);

    /**
     * Returns a Boolean value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a Boolean
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default Boolean getBoolean(String name) {
        return getBoolean(columnNumber(name));
    }

    /**
     * Returns a Byte value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @return the column value as a Byte
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    Byte getByte(int number);

    /**
     * Returns a Byte value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a Byte
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default Byte getByte(String name) {
        return getByte(columnNumber(name));
    }

    /**
     * Returns a Short value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @return the column value as a Short
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    Short getShort(int number);

    /**
     * Returns a Short value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a Short
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default Short getShort(String name) {
        return getShort(columnNumber(name));
    }

    /**
     * Returns a Character value for the given 1-based column number, performing a conversion
     * if necessary.
     *
     * @param number 1-based column number
     * @return the column value as a Character
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    Character getCharacter(int number);

    /**
     * Returns a Character value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a Character
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default Character getCharacter(String name) {
        return getCharacter(columnNumber(name));
    }

    /**
     * Returns an Integer value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @return the column value as an Integer
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    Integer getInteger(int number);

    /**
     * Returns an Integer value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as an Integer
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default Integer getInteger(String name) {
        return getInteger(columnNumber(name));
    }

    /**
     * Returns a Long value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @return the column value as a Long
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    Long getLong(int number);

    /**
     * Returns a Long value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a Long
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default Long getLong(String name) {
        return getLong(columnNumber(name));
    }

    /**
     * Returns a Float value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @return the column value as a Float
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    Float getFloat(int number);

    /**
     * Returns a Float value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a Float
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default Float getFloat(String name) {
        return getFloat(columnNumber(name));
    }

    /**
     * Returns a Double value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @return the column value as a Double
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    Double getDouble(int number);

    /**
     * Returns a Double value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a Double
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default Double getDouble(String name) {
        return getDouble(columnNumber(name));
    }

    /**
     * Returns a String value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @return the column value as a String
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     */
    String getString(int number);

    /**
     * Returns a String value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a String
     * @throws IllegalArgumentException if the given column name is unknown
     */
    default String getString(String name) {
        return getString(columnNumber(name));
    }

    /**
     * Returns a BigInteger value for the given 1-based column number, performing a conversion
     * if necessary.
     *
     * @param number 1-based column number
     * @return the column value as a BigInteger
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    BigInteger getBigInteger(int number);

    /**
     * Returns a BigInteger value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a BigInteger
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default BigInteger getBigInteger(String name) {
        return getBigInteger(columnNumber(name));
    }

    /**
     * Returns a BigDecimal value for the given 1-based column number, performing a conversion
     * if necessary.
     *
     * @param number 1-based column number
     * @return the column value as a BigDecimal
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    BigDecimal getBigDecimal(int number);

    /**
     * Returns a BigDecimal value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a BigDecimal
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default BigDecimal getBigDecimal(String name) {
        return getBigDecimal(columnNumber(name));
    }

    /**
     * Sets a value for the given 1-based column number, performing a conversion if necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, Object value);

    /**
     * Sets a value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, Object value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a boolean value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, boolean value);

    /**
     * Sets a boolean value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, boolean value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a boolean array value for the given 1-based column number, performing a conversion
     * if necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, boolean[] value);

    /**
     * Sets a boolean array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, boolean[] value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a byte value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, byte value);

    /**
     * Sets a byte value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, byte value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a byte array value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, byte[] value);

    /**
     * Sets a byte array value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, byte[] value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a short value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, short value);

    /**
     * Sets a short value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, short value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a short array value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, short[] value);

    /**
     * Sets a short array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, short[] value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a char value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, char value);

    /**
     * Sets a char value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, char value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a char array value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, char[] value);

    /**
     * Sets a char array value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, char[] value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets an int value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, int value);

    /**
     * Sets an int value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, int value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets an int array value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, int[] value);

    /**
     * Sets an int array value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, int[] value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a long value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, long value);

    /**
     * Sets a long value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, long value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a long array value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, long[] value);

    /**
     * Sets a long array value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, long[] value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a float value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, float value);

    /**
     * Sets a float value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, float value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a float array value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, float[] value);

    /**
     * Sets a float array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, float[] value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a double value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, double value);

    /**
     * Sets a double value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, double value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a double array value for the given 1-based column number, performing a conversion
     * if necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, double[] value);

    /**
     * Sets a double array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, double[] value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a Boolean value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, Boolean value);

    /**
     * Sets a Boolean value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, Boolean value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a Byte value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, Byte value);

    /**
     * Sets a Byte value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, Byte value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a Short value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, Short value);

    /**
     * Sets a Short value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, Short value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a Character value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, Character value);

    /**
     * Sets a Character value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, Character value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets an Integer value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, Integer value);

    /**
     * Sets an Integer value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, Integer value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a Long value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, Long value);

    /**
     * Sets a Long value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, Long value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a Float value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, Float value);

    /**
     * Sets a Float value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, Float value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a Double value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, Double value);

    /**
     * Sets a Double value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, Double value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a String value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, String value);

    /**
     * Sets a String value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, String value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a BigInteger value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, BigInteger value);

    /**
     * Sets a BigInteger value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, BigInteger value) {
        set(columnNumber(name), value);
    }

    /**
     * Sets a BigDecimal value for the given 1-based column number, performing a conversion if
     * necessary.
     *
     * @param number 1-based column number
     * @param value the column value to set
     * @throws IndexOutOfBoundsException if the given column number is out of bounds
     * @throws ConversionException
     */
    void set(int number, BigDecimal value);

    /**
     * Sets a BigDecimal value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    default void set(String name, BigDecimal value) {
        set(columnNumber(name), value);
    }
}
