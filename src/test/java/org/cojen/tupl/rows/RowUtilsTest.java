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

import java.math.BigInteger;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.TestUtils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RowUtilsTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RowUtilsTest.class.getName());
    }

    @Test
    public void stringUTF() {
        var rnd = new Random(8136467748116320326L);

        for (int i=0; i<1000; i++) {
            stringUTF(rnd, 0, 1000, 0xd7ff);
        }

        for (int i=0; i<1000; i++) {
            stringUTF(rnd, 0, 1000);
        }
    }

    private void stringUTF(Random rnd, int minLen, int maxLen) {
        stringUTF(rnd, minLen, maxLen, Character.MAX_CODE_POINT);
    }

    private void stringUTF(Random rnd, int minLen, int maxLen, int maxCodePoint) {
        String str = RowTestUtils.randomString(rnd, minLen, maxLen, maxCodePoint);

        byte[] encoded;
        {
            int len = RowUtils.lengthStringUTF(str);
            encoded = new byte[RowUtils.lengthPrefixPF(len) + len];
            int offset = RowUtils.encodePrefixPF(encoded, 0, len);
            offset = RowUtils.encodeStringUTF(encoded, offset, str);
            assertEquals(encoded.length, offset);
        }

        int len = RowUtils.decodePrefixPF(encoded, 0);
        String decoded = RowUtils.decodeStringUTF(encoded, RowUtils.lengthPrefixPF(len), len);

        assertEquals(str, decoded);

        assertEquals(encoded.length, RowUtils.skipBytesPF(encoded, 0));
    }

    @Test
    public void stringKey() {
        stringKey(false);
    }

    @Test
    public void stringKeyDesc() {
        stringKey(true);
    }

    private void stringKey(boolean desc) {
        var rnd = new Random(4365914224358570741L);

        var all = new TreeMap<String, byte[]>(new CodePointComparator());

        for (int i=0; i<1000; i++) {
            stringKey(all, desc, rnd, 0, 1000, 0xd7ff);
        }

        for (int i=0; i<1000; i++) {
            stringKey(all, desc, rnd, 0, 1000);
        }

        var allEncoded = new byte[all.size()][];
        {
            int i = 0;
            for (byte[] encoded : all.values()) {
                allEncoded[i++] = encoded;
            }
            Arrays.sort(allEncoded, Arrays::compareUnsigned);
        }

        if (desc) {
            int i = 0;
            for (Map.Entry<String, byte[]> e : all.descendingMap().entrySet()) {
                TestUtils.fastAssertArrayEquals(e.getValue(), allEncoded[i++]);
            }
        } else {
            int i = 0;
            for (Map.Entry<String, byte[]> e : all.entrySet()) {
                TestUtils.fastAssertArrayEquals(e.getValue(), allEncoded[i++]);
            }
        }
    }

    private void stringKey(Map<String, byte[]> all, boolean desc,
                           Random rnd, int minLen, int maxLen)
    {
        stringKey(all, desc, rnd, minLen, maxLen, Character.MAX_CODE_POINT);
    }

    private void stringKey(Map<String, byte[]> all, boolean desc,
                           Random rnd, int minLen, int maxLen, int maxCodePoint)
    {
        String str;
        do {
            str = RowTestUtils.randomString(rnd, minLen, maxLen, maxCodePoint);
        } while (all.containsKey(str));

        byte[] encoded;
        {
            int len = RowUtils.lengthStringKey(str);

            encoded = new byte[len];
            int offset;
            if (desc) {
                offset = RowUtils.encodeStringKeyDesc(encoded, 0, str);
            } else {
                offset = RowUtils.encodeStringKey(encoded, 0, str);
            }

            assertEquals(encoded.length, offset);
        }

        assertEquals(encoded.length, RowUtils.lengthStringKey(encoded, 0));

        if (desc) {
            assertEquals(str, RowUtils.decodeStringKeyDesc(encoded, 0, encoded.length));
        } else {
            assertEquals(str, RowUtils.decodeStringKey(encoded, 0, encoded.length));
        }

        all.put(str, encoded);
    }

    /**
     * Java Strings are naturally ordered by UTF-16 chars, but encodeStringKey orders by
     * Unicode codepoints, which is more accurate.
     */
    static class CodePointComparator implements Comparator<String> {
        @Override
        public int compare(String a, String b) {
            return Arrays.compare(a.codePoints().toArray(), b.codePoints().toArray());
        }
    }

    @Test
    public void bigIntegerKey() {
        bigIntegerKey(false);
    }

    @Test
    public void bigIntegerKeyDesc() {
        bigIntegerKey(true);
    }

    private void bigIntegerKey(boolean desc) {
        var rnd = new Random(1316229822956025521L);

        var all = new TreeMap<BigInteger, byte[]>();

        for (int i=0; i<1000; i++) {
            bigIntegerKey(all, desc, rnd, 500);
        }

        var allEncoded = new byte[all.size()][];
        {
            int i = 0;
            for (byte[] encoded : all.values()) {
                allEncoded[i++] = encoded;
            }
            Arrays.sort(allEncoded, Arrays::compareUnsigned);
        }

        if (desc) {
            int i = 0;
            for (Map.Entry<BigInteger, byte[]> e : all.descendingMap().entrySet()) {
                TestUtils.fastAssertArrayEquals(e.getValue(), allEncoded[i++]);
            }
        } else {
            int i = 0;
            for (Map.Entry<BigInteger, byte[]> e : all.entrySet()) {
                TestUtils.fastAssertArrayEquals(e.getValue(), allEncoded[i++]);
            }
        }
    }

    private void bigIntegerKey(Map<BigInteger, byte[]> all, boolean desc, Random rnd, int maxLen) {
        BigInteger bi;
        do {
            bi = RowTestUtils.randomBigInteger(rnd, maxLen);
        } while (all.containsKey(bi));

        byte[] encoded;
        {
            var bytes = bi.toByteArray();

            int len = bytes.length;
            if (len >= 0x7f) {
                len += 4;
            }

            encoded = new byte[1 + len];

            int offset;
            if (desc) {
                offset = RowUtils.encodeBigIntegerKeyDesc(encoded, 0, bytes);
            } else {
                offset = RowUtils.encodeBigIntegerKey(encoded, 0, bytes);
            }

            assertEquals(encoded.length, offset);
        }

        long result = RowUtils.decodeBigIntegerKeyHeader(encoded, 0);

        int offset = (int) result;
        int remaining = (int) (result >> 32);

        assertEquals(encoded.length, offset + remaining);

        if (desc) {
            RowUtils.flip(encoded, offset, remaining);
        }

        var decoded = new BigInteger(encoded, offset, remaining);

        if (desc) {
            RowUtils.flip(encoded, offset, remaining);
        }

        assertEquals(bi, decoded);

        all.put(bi, encoded);
    }
}
