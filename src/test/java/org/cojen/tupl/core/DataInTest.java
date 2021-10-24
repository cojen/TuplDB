/*
 *  Copyright (C) 2019 Cojen.org
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

package org.cojen.tupl.core;

import java.io.ByteArrayInputStream;
import java.util.Random;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class DataInTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(DataInTest.class.getName());
    }

    @Test
    public void fromEmpty() throws Exception {
        // Read when nothing has been buffered yet.

        var rnd = new Random();
        byte[] src = randomStr(rnd, 500);
        var bin = new ByteArrayInputStream(src);
        var din = new DataIn.Stream(0, bin, 100);

        var buf = new byte[500];
        assertEquals(500, din.read(buf));
        fastAssertArrayEquals(src, buf);

        assertEquals(-1, din.read(buf));
    }

    @Test
    public void fromEmptyEnd() throws Exception {
        // Read from an empty buffer when no more data is left.

        var bin = new ByteArrayInputStream(new byte[0]);
        var din = new DataIn.Stream(0, bin, 100);

        var buf = new byte[50];
        assertEquals(-1, din.read(buf));
    }

    @Test
    public void split() throws Exception {
        // Read into the buffer, and then read partially.

        var rnd = new Random();
        byte[] src = randomStr(rnd, 100);
        var bin = new ByteArrayInputStream(src);
        var din = new DataIn.Stream(0, bin, 100);

        var buf = new byte[100];
        assertEquals(10, din.read(buf, 0, 10));
        assertEquals(80, din.read(buf, 10, 80));
        assertEquals(10, din.read(buf, 90, 20));
        fastAssertArrayEquals(src, buf);

        assertEquals(-1, din.read(buf));
    }

    @Test
    public void unsignedInt() throws Exception {
        var src = new byte[100];
        int off = 0;
        off = Utils.encodeUnsignedVarInt(src, off, 1);
        off = Utils.encodeUnsignedVarInt(src, off, 130);
        off = Utils.encodeUnsignedVarInt(src, off, 50_000);
        off = Utils.encodeUnsignedVarInt(src, off, 100_000_000);
        off = Utils.encodeUnsignedVarInt(src, off, Integer.MAX_VALUE + 10);
        assertEquals(15, off);

        var bin = new ByteArrayInputStream(src);
        var din = new DataIn.Stream(0, bin, 100);

        assertEquals(1, din.readUnsignedVarInt());
        assertEquals(130, din.readUnsignedVarInt());
        assertEquals(50_000, din.readUnsignedVarInt());
        assertEquals(100_000_000, din.readUnsignedVarInt());
        assertEquals(Integer.MAX_VALUE + 10, din.readUnsignedVarInt());
    }

    @Test
    public void unsignedLong() throws Exception {
        var src = new byte[100];
        int off = 0;
        off = Utils.encodeUnsignedVarLong(src, off, 1);
        off = Utils.encodeUnsignedVarLong(src, off, 130);
        off = Utils.encodeUnsignedVarLong(src, off, 50_000);
        off = Utils.encodeUnsignedVarLong(src, off, 100_000_000);
        off = Utils.encodeUnsignedVarLong(src, off, 30_000_000_000L);
        off = Utils.encodeUnsignedVarLong(src, off, 35_000_000_000L);
        off = Utils.encodeUnsignedVarLong(src, off, 567_382_630_219_903L);
        off = Utils.encodeUnsignedVarLong(src, off, 600_000_000_000_000L);
        off = Utils.encodeUnsignedVarLong(src, off, ~0L);
        assertEquals(45, off);

        var bin = new ByteArrayInputStream(src);
        var din = new DataIn.Stream(0, bin, 100);

        assertEquals(1, din.readUnsignedVarLong());
        assertEquals(130, din.readUnsignedVarLong());
        assertEquals(50_000, din.readUnsignedVarLong());
        assertEquals(100_000_000, din.readUnsignedVarLong());
        assertEquals(30_000_000_000L, din.readUnsignedVarLong());
        assertEquals(35_000_000_000L, din.readUnsignedVarLong());
        assertEquals(567_382_630_219_903L, din.readUnsignedVarLong());
        assertEquals(600_000_000_000_000L, din.readUnsignedVarLong());
        assertEquals(~0L, din.readUnsignedVarLong());
    }
}
