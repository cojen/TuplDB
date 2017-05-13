/*
 *  Copyright (C) 2011-2017 Cojen.org
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

package org.cojen.tupl;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.Utils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class UtilsTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(UtilsTest.class.getName());
    }

    @Test
    public void varUnsignedInt() {
        varUnsignedInt(0, 1);
        varUnsignedInt(1, 1);
        varUnsignedInt(126, 1);
        varUnsignedInt(127, 1);

        varUnsignedInt(128, 2);
        varUnsignedInt(129, 2);
        varUnsignedInt(16510, 2);
        varUnsignedInt(16511, 2);

        varUnsignedInt(16512, 3);
        varUnsignedInt(16513, 3);
        varUnsignedInt(2113662, 3);
        varUnsignedInt(2113663, 3);

        varUnsignedInt(2113664, 4);
        varUnsignedInt(270549119, 4);

        varUnsignedInt(270549120, 5);
        varUnsignedInt(Integer.MAX_VALUE, 5);

        varUnsignedInt(-1, 5);
        varUnsignedInt(-1000, 5);
        varUnsignedInt(Integer.MIN_VALUE, 5);
    }

    private void varUnsignedInt(int value, int size) {
        assertEquals(size, calcUnsignedVarIntLength(value));

        byte[] b = new byte[size];
        int offset = encodeUnsignedVarInt(b, 0, value);
        assertEquals(b.length, offset);

        int decoded = decodeUnsignedVarInt(b, 0);
        assertEquals(value, decoded);
    }

    @Test
    public void varSignedInt() {
        varSignedInt(-64, 1);
        varSignedInt(-63, 1);
        varSignedInt(62, 1);
        varSignedInt(63, 1);

        varSignedInt(-8256, 2);
        varSignedInt(-8255, 2);
        varSignedInt(-66, 2);
        varSignedInt(-65, 2);
        varSignedInt(64, 2);
        varSignedInt(65, 2);
        varSignedInt(8254, 2);
        varSignedInt(8255, 2);

        varSignedInt(-1056832, 3);
        varSignedInt(-1056831, 3);
        varSignedInt(-8258, 3);
        varSignedInt(-8257, 3);
        varSignedInt(8256, 3);
        varSignedInt(8257, 3);
        varSignedInt(1056830, 3);
        varSignedInt(1056831, 3);

        varSignedInt(-135274560, 4);
        varSignedInt(-135274559, 4);
        varSignedInt(-1056834, 4);
        varSignedInt(-1056833, 4);
        varSignedInt(1056832, 4);
        varSignedInt(1056833, 4);
        varSignedInt(135274558, 4);
        varSignedInt(135274559, 4);

        varSignedInt(Integer.MIN_VALUE, 5);
        varSignedInt(Integer.MIN_VALUE + 1, 5);
        varSignedInt(-135274562, 5);
        varSignedInt(-135274561, 5);
        varSignedInt(135274560, 5);
        varSignedInt(135274561, 5);
        varSignedInt(Integer.MAX_VALUE - 1, 5);
        varSignedInt(Integer.MAX_VALUE, 5);
    }

    private void varSignedInt(int value, int size) {
        byte[] b = new byte[size];
        int offset = encodeSignedVarInt(b, 0, value);
        assertEquals(b.length, offset);

        int decoded = decodeSignedVarInt(b, 0);
        assertEquals(value, decoded);
    }

    @Test
    public void varUnsignedLong() {
        varUnsignedLong(0, 1);
        varUnsignedLong(1, 1);
        varUnsignedLong(126, 1);
        varUnsignedLong(127, 1);

        varUnsignedLong(128, 2);
        varUnsignedLong(129, 2);
        varUnsignedLong(16510, 2);
        varUnsignedLong(16511, 2);

        varUnsignedLong(16512, 3);
        varUnsignedLong(16513, 3);
        varUnsignedLong(2113662, 3);
        varUnsignedLong(2113663, 3);

        varUnsignedLong(2113664, 4);
        varUnsignedLong(270549119, 4);

        varUnsignedLong(270549120, 5);
        varUnsignedLong(Integer.MAX_VALUE, 5);
        varUnsignedLong(34630287487L, 5);

        varUnsignedLong(34630287488L, 6);
        varUnsignedLong(4432676798591L, 6);

        varUnsignedLong(4432676798592L, 7);
        varUnsignedLong(567382630219903L, 7);

        varUnsignedLong(567382630219904L, 8);
        varUnsignedLong(72624976668147839L, 8);

        varUnsignedLong(72624976668147840L, 9);
        varUnsignedLong(Long.MAX_VALUE, 9);

        varUnsignedLong(-1, 9);
        varUnsignedLong(-1000, 9);
        varUnsignedLong(Integer.MIN_VALUE, 9);
        varUnsignedLong(Long.MIN_VALUE, 9);
    }

    private void varUnsignedLong(long value, int size) {
        assertEquals(size, calcUnsignedVarLongLength(value));

        byte[] b = new byte[size];
        int offset = encodeUnsignedVarLong(b, 0, value);
        assertEquals(b.length, offset);

        class Ref implements IntegerRef {
            int off;

            public int get() {
                return off;
            }

            public void set(int v) {
                off = v;
            }
        };

        Ref ref = new Ref();
        ref.off = 0;

        long decoded = decodeUnsignedVarLong(b, ref);
        assertEquals(value, decoded);

        assertEquals(size, ref.off);
    }

    @Test
    public void shortBE() {
        byte[] b = new byte[2];
        encodeShortBE(b, 0, 0x8182);
        assertArrayEquals(new byte[] {(byte) 0x81, (byte) 0x82}, b);
        assertEquals(0x8182, decodeUnsignedShortBE(b, 0));
    }

    @Test
    public void shortLE() {
        byte[] b = new byte[2];
        encodeShortLE(b, 0, 0x8182);
        assertArrayEquals(new byte[] {(byte) 0x82, (byte) 0x81}, b);
        assertEquals(0x8281, decodeUnsignedShortBE(b, 0));
    }

    @Test
    public void intBE() {
        byte[] b = new byte[4];
        encodeIntBE(b, 0, 0x81828384);
        assertArrayEquals(new byte[] {(byte) 0x81, (byte) 0x82, (byte) 0x83, (byte) 0x84}, b);
        assertEquals(0x81828384, decodeIntBE(b, 0));
    }

    @Test
    public void intLE() {
        byte[] b = new byte[4];
        encodeIntLE(b, 0, 0x81828384);
        assertArrayEquals(new byte[] {(byte) 0x84, (byte) 0x83, (byte) 0x82, (byte) 0x81}, b);
        assertEquals(0x81828384, decodeIntLE(b, 0));
    }

    @Test
    public void int48BE() {
        byte[] b = new byte[6];
        encodeInt48BE(b, 0, 0x818283848586L);
        assertArrayEquals(new byte[] {(byte) 0x81, (byte) 0x82, (byte) 0x83, (byte) 0x84,
                                      (byte) 0x85, (byte) 0x86}, b);
        assertEquals(0x818283848586L, decodeUnsignedInt48BE(b, 0));
    }

    @Test
    public void int48LE() {
        byte[] b = new byte[6];
        encodeInt48LE(b, 0, 0x818283848586L);
        assertArrayEquals(new byte[] {(byte) 0x86, (byte) 0x85, (byte) 0x84, (byte) 0x83,
                                      (byte) 0x82, (byte) 0x81}, b);
        assertEquals(0x818283848586L, decodeUnsignedInt48LE(b, 0));
    }

    @Test
    public void longBE() {
        byte[] b = new byte[8];
        encodeLongBE(b, 0, 0x8182838485868788L);
        assertArrayEquals(new byte[] {(byte) 0x81, (byte) 0x82, (byte) 0x83, (byte) 0x84,
                                      (byte) 0x85, (byte) 0x86, (byte) 0x87, (byte) 0x88}, b);
        assertEquals(0x8182838485868788L, decodeLongBE(b, 0));
    }

    @Test
    public void longLE() {
        byte[] b = new byte[8];
        encodeLongLE(b, 0, 0x8182838485868788L);
        assertArrayEquals(new byte[] {(byte) 0x88, (byte) 0x87, (byte) 0x86, (byte) 0x85,
                                      (byte) 0x84, (byte) 0x83, (byte) 0x82, (byte) 0x81}, b);
        assertEquals(0x8182838485868788L, decodeLongLE(b, 0));
    }
}
