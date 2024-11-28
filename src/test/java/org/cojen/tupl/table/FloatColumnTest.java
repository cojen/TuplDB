/*
 *  Copyright (C) 2022 Cojen.org
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

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class FloatColumnTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(FloatColumnTest.class.getName());
    }

    @Test
    public void preciseMatch() throws Exception {
        // Test that searches for 0.0 and NaN are precise and consistent with an index search.
        // This behavior is different than the Java equals operator, which considers -0.0 to be
        // equal to 0.0, and all NaN comparisons are false. A range search is required to be
        // less precise.

        // Also see BigDecimalColumnTest. BigDecimal comparisons are range based automatically
        // because the the number of trailing zeros is unbounded, and this range cannot be
        // specified manually.

        Database db = Database.open(new DatabaseConfig());

        Table<Rec1> table1 = db.openTable(Rec1.class);
        Table<Rec2> table2 = db.openTable(Rec2.class);

        final float NaN1 = Float.intBitsToFloat(2143289344); // canonical NaN
        final float NaN2 = Float.intBitsToFloat(2143289345);

        float[] values = {-0.0f, 0.0f, NaN1, NaN2};

        for (int i=0; i<values.length; i++) {
            var row = table1.newRow();
            row.id(i);
            row.value(values[i]);
            table1.insert(null, row);
        }

        for (int i=0; i<values.length; i++) {
            var row = table2.newRow();
            row.id(i);
            row.value(values[i]);
            table2.insert(null, row);
        }

        for (float v : values) {
            assertEquals(1, count(table1, "value == ?", v));
        }

        for (float v : values) {
            assertEquals(1, count(table2, "value == ?", v));
        }

        assertEquals(2, count(table1, "value >= ? && value <= ?", -0.0f, 0.0f));
        assertEquals(2, count(table2, "value >= ? && value <= ?", -0.0f, 0.0f));

        assertEquals(0, count(table1, "value >= ? && value <= ?", 0.0f, -0.0f));
        assertEquals(0, count(table2, "value >= ? && value <= ?", 0.0f, -0.0f));

        assertEquals(2, count(table1, "value >= ? && value <= ?", NaN1, NaN2));
        assertEquals(2, count(table2, "value >= ? && value <= ?", NaN1, NaN2));

        assertEquals(0, count(table1, "value >= ? && value <= ?", NaN2, NaN1));
        assertEquals(0, count(table2, "value >= ? && value <= ?", NaN2, NaN1));

        db.close();
    }

    private static long count(Table<?> table, String filter, Object... args) throws Exception {
        return table.newStream(null, filter, args).count();
    }

    @PrimaryKey("id")
    public interface Rec1 {
        int id();
        void id(int i);

        float value();
        void value(float f);
    }

    @PrimaryKey("id")
    @SecondaryIndex("value")
    public interface Rec2 extends Rec1 {
    }
}
