/*
 *  Copyright (C) 2023 Cojen.org
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

package org.cojen.tupl.sql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLNonTransientException;
import java.sql.Types;

import java.util.LinkedHashMap;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.rows.RowMaker;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ResultSetTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ResultSetTest.class.getName());
    }


    @Test
    public void basic() throws Exception {
        var rsClass = ResultSetMaker.find(Row.class, null);
        var rsCtor = rsClass.getConstructor();

        {
            var rs = (ResultSet) rsCtor.newInstance();
            assertFalse(rs.isClosed());
            assertTrue(rs.toString().contains("ResultSet"));
            try {
                rs.getInt(1);
                fail();
            } catch (SQLNonTransientException e) {
                assertTrue(e.getMessage().contains("positioned"));
            }
            assertEquals(1, rs.findColumn("id"));
            try {
                rs.findColumn("xxx");
                fail();
            } catch (SQLNonTransientException e) {
                assertTrue(e.getMessage().contains("doesn't exist"));
            }
            assertFalse(rs.next());
            assertTrue(rs.isClosed());
            try {
                rs.getInt(1);
                fail();
            } catch (SQLNonTransientException e) {
                assertTrue(e.getMessage().contains("closed"));
            }
            try {
                rs.getString(null);
            } catch (SQLNonTransientException e) {
                assertTrue(e.getMessage().toLowerCase().contains("null"));
            }
        }

        {
            var rs = (ResultSet) rsCtor.newInstance();
            var rowClass = RowMaker.find(Row.class);
            var row = rowClass.getConstructor().newInstance();
            rsClass.getMethod("init", rowClass).invoke(rs, row);

            assertTrue(rs.toString().contains("ResultSet"));
            assertTrue(rs.toString().endsWith("{}"));

            rs.updateString("id", "123");
            rs.updateNull("value");
            rs.updateInt("number", 9);
            rs.updateObject("text", "hello");

            assertTrue(rs.toString().endsWith("{*id=123, *number=9, *value=null, *text=hello}"));

            assertEquals(123L, rs.getInt(1));
            assertFalse(rs.wasNull());
            assertEquals(123L, rs.getLong("id"));
            assertFalse(rs.wasNull());
            assertEquals(123L, rs.getLong("ID"));
            assertEquals(123L, rs.getLong("Id"));
            assertEquals(123L, rs.getLong("iD"));
            assertNull(rs.getObject("value"));
            assertTrue(rs.wasNull());
            assertEquals(0, rs.getInt("value"));
            assertTrue(rs.wasNull());
            assertEquals("hello", rs.getString("text"));
            assertFalse(rs.wasNull());
            assertNull(rs.getString("value"));
            assertTrue(rs.wasNull());

            assertFalse(rs.next());
            assertTrue(rs.isClosed());
            try {
                rs.getInt(1);
                fail();
            } catch (SQLNonTransientException e) {
                assertTrue(e.getMessage().contains("closed"));
            }

            assertTrue(!rs.toString().contains("}"));
        }

        {
            var rs = (ResultSet) rsCtor.newInstance();
            var md = rs.getMetaData();

            assertEquals(4, md.getColumnCount());

            int nullable = ResultSetMetaData.columnNullable;
            int noNulls = ResultSetMetaData.columnNoNulls;
            int max = Integer.MAX_VALUE;

            Object[] expect = {
                "id", true, noNulls, true, 20, 0, 20, Types.BIGINT,
                "number", false, noNulls, false, 10, 0, 10, Types.INTEGER,
                "text", false, noNulls, false, max, 0, max, Types.VARCHAR,
                "value", false, nullable, true, 11, 0, 10, Types.INTEGER,
            };

            for (int i=1; i<=4; i++) {
                int pos = (i - 1) * 8;
                assertEquals(expect[pos++], md.getColumnName(i));
                assertEquals(expect[pos++], md.isAutoIncrement(i));
                assertEquals(expect[pos++], md.isNullable(i));
                assertEquals(expect[pos++], md.isSigned(i));
                assertEquals(expect[pos++], md.getColumnDisplaySize(i));
                assertEquals(expect[pos++], md.getScale(i));
                assertEquals(expect[pos++], md.getPrecision(i));
                assertEquals(expect[pos++], md.getColumnType(i));
            }
        }
    }

    @Test
    public void projection() throws Exception {
        var projection = new LinkedHashMap<String, String>();
        projection.put("id", "idx");
        projection.put("text", "message");

        var rsClass = ResultSetMaker.find(Row.class, projection);
        var rsCtor = rsClass.getConstructor();
        var rs = (ResultSet) rsCtor.newInstance();
        var rowClass = RowMaker.find(Row.class);
        var row = rowClass.getConstructor().newInstance();
        rsClass.getMethod("init", rowClass).invoke(rs, row);

        var md = rs.getMetaData();
        assertEquals(2, md.getColumnCount());
        assertEquals("idx", md.getColumnName(1));
        assertEquals("message", md.getColumnName(2));
        try {
            md.getColumnName(3);
            fail();
        } catch (SQLNonTransientException e) {
            assertTrue(e.getMessage().contains("doesn't exist"));
        }

        rs.updateLong("idx", 123);
        try {
            rs.updateNull("message");
            fail();
        } catch (SQLNonTransientException e) {
            assertTrue(e.getMessage().contains("cannot be set to null"));
        }

        assertEquals(123, rs.getLong(1));
        assertNull(rs.getString(2));
        assertFalse(rs.wasNull()); // strange artifact of acting upon a directly passed-in row

        rs.updateString("message", "hello");
        assertEquals("hello", rs.getString(2));
        assertEquals("hello", rs.getString("message"));
    }

    @PrimaryKey("id")
    public static interface Row {
        @Automatic
        long id();
        void id(long id);

        @Nullable
        Integer value();
        void value(Integer v);

        @Unsigned
        int number();
        void number(int n);

        String text();
        void text(String t);
    }
}
