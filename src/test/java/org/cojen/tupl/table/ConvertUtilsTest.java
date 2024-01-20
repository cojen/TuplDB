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

package org.cojen.tupl.table;

import java.util.Arrays;

import org.cojen.tupl.table.filter.ColumnFilter;

import static org.cojen.tupl.table.ColumnInfo.*;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ConvertUtilsTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ConvertUtilsTest.class.getName());
    }

    @Test
    public void numberConversions() throws Exception {
        numberConversions(0, 0);
        numberConversions(0, TYPE_NULLABLE);
        numberConversions(TYPE_NULLABLE, 0);
        numberConversions(TYPE_NULLABLE, TYPE_NULLABLE);
    }

    private void numberConversions(int aFlags, int bFlags) throws Exception {
        // Verifies that the common numerical type has enough bits, but doesn't verify if it's
        // the smallest possible.

        NumType[] numTypes = allNumTypes();

        for (NumType aNumType : numTypes) {
            var aInfo = new ColumnInfo();
            aInfo.typeCode = aNumType.typeCode | aFlags;
            aInfo.assignType();

            for (NumType bNumType : numTypes) {
                var bInfo = new ColumnInfo();
                bInfo.typeCode = bNumType.typeCode | bFlags;
                bInfo.assignType();

                ColumnInfo cInfo = ConvertUtils.commonType(aInfo, bInfo, ColumnFilter.OP_EQ);

                var cNumType = findNumType(numTypes, cInfo.typeCode);

                assertTrue(cNumType.canHold(aNumType));
                assertTrue(cNumType.canHold(bNumType));

                if (isNullable(aInfo.typeCode) || isNullable(bInfo.typeCode)) {
                    assertTrue(isNullable(cInfo.typeCode));
                } else if (!isNullable(aInfo.typeCode) && !isNullable(bInfo.typeCode)) {
                    assertFalse(isNullable(cInfo.typeCode));
                }
            }
        }

    }

    @Test
    public void stringConversions() throws Exception {
        stringConversions(0, 0);
        stringConversions(0, TYPE_NULLABLE);
        stringConversions(TYPE_NULLABLE, 0);
        stringConversions(TYPE_NULLABLE, TYPE_NULLABLE);
    }

    private void stringConversions(int aFlags, int bFlags) throws Exception {
        NumType[] numTypes = allNumTypes();

        for (NumType aNumType : numTypes) {
            var aInfo = new ColumnInfo();
            aInfo.typeCode = aNumType.typeCode | aFlags;
            aInfo.assignType();

            var bInfo = new ColumnInfo();
            bInfo.typeCode = TYPE_UTF8 | bFlags;
            bInfo.assignType();

            ColumnInfo cInfo = ConvertUtils.commonType(aInfo, bInfo, ColumnFilter.OP_EQ);
            ColumnInfo cInfo2 = ConvertUtils.commonType(bInfo, aInfo, ColumnFilter.OP_EQ);
            assertEquals(cInfo.typeCode, cInfo2.typeCode);

            if (isNullable(aInfo.typeCode) || isNullable(bInfo.typeCode)) {
                assertEquals((TYPE_UTF8 | TYPE_NULLABLE), cInfo.typeCode);
            } else if (!isNullable(aInfo.typeCode) && !isNullable(bInfo.typeCode)) {
                assertEquals(TYPE_UTF8, cInfo.typeCode);
            }

            cInfo = ConvertUtils.commonType(aInfo, bInfo, ColumnFilter.OP_GT);
            cInfo2 = ConvertUtils.commonType(bInfo, aInfo, ColumnFilter.OP_GT);

            if (cInfo == null) {
                // Ordered comparison between string and number is ambiguous.
                assertTrue(aInfo.plainTypeCode() != TYPE_CHAR);
                assertNull(cInfo2);
            } else {
                assertEquals(TYPE_CHAR, aInfo.plainTypeCode());
                assertEquals(cInfo.typeCode, cInfo2.typeCode);

                if (isNullable(aInfo.typeCode) || isNullable(bInfo.typeCode)) {
                    assertEquals((TYPE_UTF8 | TYPE_NULLABLE), cInfo.typeCode);
                } else if (!isNullable(aInfo.typeCode) && !isNullable(bInfo.typeCode)) {
                    assertEquals(TYPE_UTF8, cInfo.typeCode);
                }
            }
        }
    }

    /**
     * @param numTypes must be sorted
     */
    static NumType findNumType(NumType[] numTypes, int typeCode) {
        typeCode = plainTypeCode(typeCode);
        int index = Arrays.binarySearch(numTypes, new NumType(typeCode, 0, 0, 0, 0));
        return numTypes[index];
    }

    static NumType[] allNumTypes() {
        NumType[] numTypes = {
            new NumType(TYPE_UBYTE, 8, 0, 0, 0),
            new NumType(TYPE_USHORT, 16, 0, 0, 0),
            new NumType(TYPE_UINT, 32, 0, 0, 0),
            new NumType(TYPE_ULONG, 64, 0, 0, 0),
            new NumType(TYPE_BYTE, 7, 7, 0, 0),
            new NumType(TYPE_SHORT, 15, 15, 0, 0),
            new NumType(TYPE_INT, 31, 31, 0, 0),
            new NumType(TYPE_LONG, 63, 63, 0, 0),
            new NumType(TYPE_FLOAT, 23, 23, 23, 8),
            new NumType(TYPE_DOUBLE, 52, 52, 52, 11),
            new NumType(TYPE_CHAR, 16, 0, 0, 0),
            new NumType(TYPE_BIG_INTEGER, 256, 256, 0, 0), // 256 is big enough
            new NumType(TYPE_BIG_DECIMAL, 256, 256, 256, 256),
        };

        Arrays.sort(numTypes);
        return numTypes;
    }

    record NumType(int typeCode, int positive, int negative, int fraction, int exponent)
        implements Comparable<NumType>
    {
        boolean canHold(NumType other) {
            return positive >= other.positive && negative >= other.negative
                    && fraction >= other.fraction && exponent >= other.exponent;
        }

        @Override
        public int compareTo(NumType other) {
            return Integer.compare(typeCode, other.typeCode);
        }
    }
}
