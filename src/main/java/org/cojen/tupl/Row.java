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
 * If the row represents a join, dotted name paths can be used to access sub columns.
 *
 * @author Brian S. O'Neill
 * @see Table
 */
public interface Row {
    /**
     * Returns the column type for the given column name.
     *
     * @param name column name
     * @return column type
     * @throws IllegalArgumentException if the given column name is unknown
     */
    Class<?> columnType(String name);

    /**
     * Returns the method name for the given column name. The method name can differ from the
     * column name if it needed to be renamed to something legal.
     *
     * @param name column name
     * @return column method name
     * @throws IllegalArgumentException if the given column name is unknown
     */
    default String columnMethodName(String name) {
        return name;
    }

    /**
     * Returns the value for the given column name.
     *
     * @param name column name
     * @return the column value, possibly boxed
     * @throws IllegalArgumentException if the given column name is unknown
     */
    Object get(String name);

    /**
     * Returns a boolean value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a boolean
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    boolean get_boolean(String name);

    /**
     * Returns a boolean array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a boolean array
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    boolean[] get_boolean_array(String name);

    /**
     * Returns a byte value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a byte
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    byte get_byte(String name);

    /**
     * Returns a byte array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a byte array
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    byte[] get_byte_array(String name);

    /**
     * Returns a short value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a short
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    short get_short(String name);

    /**
     * Returns a short array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a short array
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    short[] get_short_array(String name);

    /**
     * Returns a char value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a char
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    char get_char(String name);

    /**
     * Returns a char array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a char array
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    char[] get_char_array(String name);

    /**
     * Returns an int value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as an int
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    int get_int(String name);

    /**
     * Returns an int array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as an int array
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    int[] get_int_array(String name);

    /**
     * Returns a long value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a long
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    long get_long(String name);

    /**
     * Returns a long array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a long array
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    long[] get_long_array(String name);

    /**
     * Returns a float value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a float
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    float get_float(String name);

    /**
     * Returns a float array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a float array
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    float[] get_float_array(String name);

    /**
     * Returns a double value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a double
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    double get_double(String name);

    /**
     * Returns a double array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a double array
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    double[] get_double_array(String name);

    /**
     * Returns a Boolean value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a Boolean
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    Boolean getBoolean(String name);

    /**
     * Returns a Byte value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a Byte
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    Byte getByte(String name);

    /**
     * Returns a Short value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a Short
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    Short getShort(String name);

    /**
     * Returns a Character value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a Character
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    Character getCharacter(String name);

    /**
     * Returns an Integer value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as an Integer
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    Integer getInteger(String name);

    /**
     * Returns a Long value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a Long
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    Long getLong(String name);

    /**
     * Returns a Float value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a Float
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    Float getFloat(String name);

    /**
     * Returns a Double value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a Double
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    Double getDouble(String name);

    /**
     * Returns a String value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @return the column value as a String
     * @throws IllegalArgumentException if the given column name is unknown
     */
    String getString(String name);

    /**
     * Returns a BigInteger value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a BigInteger
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    BigInteger getBigInteger(String name);

    /**
     * Returns a BigDecimal value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @return the column value as a BigDecimal
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    BigDecimal getBigDecimal(String name);

    /**
     * Sets a value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, Object value);

    /**
     * Sets a boolean value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, boolean value);

    /**
     * Sets a boolean array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, boolean[] value);

    /**
     * Sets a byte value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, byte value);

    /**
     * Sets a byte array value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, byte[] value);

    /**
     * Sets a short value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, short value);

    /**
     * Sets a short array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, short[] value);

    /**
     * Sets a char value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, char value);

    /**
     * Sets a char array value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, char[] value);

    /**
     * Sets an int value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, int value);

    /**
     * Sets an int array value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, int[] value);

    /**
     * Sets a long value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, long value);

    /**
     * Sets a long array value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, long[] value);

    /**
     * Sets a float value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, float value);

    /**
     * Sets a float array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, float[] value);

    /**
     * Sets a double value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, double value);

    /**
     * Sets a double array value for the given column name, performing a conversion if
     * necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, double[] value);

    /**
     * Sets a Boolean value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, Boolean value);

    /**
     * Sets a Byte value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, Byte value);

    /**
     * Sets a Short value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, Short value);

    /**
     * Sets a Character value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, Character value);

    /**
     * Sets an Integer value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, Integer value);

    /**
     * Sets a Long value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, Long value);

    /**
     * Sets a Float value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, Float value);

    /**
     * Sets a Double value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, Double value);

    /**
     * Sets a String value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, String value);

    /**
     * Sets a BigInteger value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, BigInteger value);

    /**
     * Sets a BigDecimal value for the given column name, performing a conversion if necessary.
     *
     * @param name column name
     * @param value the column value to set
     * @throws IllegalArgumentException if the given column name is unknown
     * @throws ConversionException
     */
    void set(String name, BigDecimal value);
}
