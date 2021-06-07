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

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.Arrays;
import java.util.Random;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class BigDecimalUtilsTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(BigDecimalUtilsTest.class.getName());
    }

    @Test
    public void bigDecimalKey() throws Exception {
        bigDecimalKey(false);
    }

    @Test
    public void bigDecimalKeyDesc() throws Exception {
        bigDecimalKey(true);
    }

    private void bigDecimalKey(boolean desc) throws Exception {
        BigDecimal[] values = {
            new BigDecimal("-123.0"),
            new BigDecimal("-123"),
            new BigDecimal("-90.01000"),
            new BigDecimal("-90.0100"),
            new BigDecimal("-90.010"),
            new BigDecimal("-90.01"),
            new BigDecimal("-11.000"),
            new BigDecimal("-11.00"),
            new BigDecimal("-11.0"),
            new BigDecimal("-11"),
            new BigDecimal("-10.100"),
            new BigDecimal("-10.10"),
            new BigDecimal("-10.1"),
            new BigDecimal("-10.010"),
            new BigDecimal("-10.01"),
            new BigDecimal("-10.001"),
            new BigDecimal("-2"),
            new BigDecimal("-1"),
            new BigDecimal("-0.1"),
            new BigDecimal("-0.01"),
            new BigDecimal("-0.001"),
            new BigDecimal("-0.0001"),
            new BigDecimal("-0.00001"),
            BigDecimal.ZERO,
            new BigDecimal("0.0"),
            new BigDecimal("0.00"),
            new BigDecimal("0.000"),
            new BigDecimal("0.00001"),
            new BigDecimal("0.0001"),
            new BigDecimal("0.001"),
            new BigDecimal("0.01"),
            new BigDecimal("0.1"),
            new BigDecimal("1"),
            new BigDecimal("2"),
            new BigDecimal("10.001"),
            new BigDecimal("10.01"),
            new BigDecimal("10.010"),
            new BigDecimal("10.1"),
            new BigDecimal("10.10"),
            new BigDecimal("10.100"),
            new BigDecimal("11"),
            new BigDecimal("11.0"),
            new BigDecimal("11.00"),
            new BigDecimal("11.000"),
            new BigDecimal("90.01"),
            new BigDecimal("90.010"),
            new BigDecimal("90.0100"),
            new BigDecimal("90.01000"),
            new BigDecimal("123"),
            new BigDecimal("123.0"),
            null
        };

        byte[][] encoded = new byte[values.length][];

        for (int i=0; i<values.length; i++) {
            if (desc) {
                encoded[i] = BigDecimalUtils.encodeBigDecimalKeyDesc(values[i]);
            } else {
                encoded[i] = BigDecimalUtils.encodeBigDecimalKey(values[i]);
            }
        }

        var ref = new BigDecimal[1];
        for (int i=0; i<values.length; i++) {
            ref[0] = BigDecimal.ZERO;
            int offset;
            if (desc) {
                offset = BigDecimalUtils.decodeBigDecimalKeyDesc(encoded[i], 0, ref);
            } else {
                offset = BigDecimalUtils.decodeBigDecimalKey(encoded[i], 0, ref);
            }
            assertEquals(values[i], ref[0]);
            assertEquals(encoded[i].length, offset);
        }

        for (int i=1; i<values.length; i++) {
            int sgn = Arrays.compareUnsigned(encoded[i - 1], encoded[i]);
            if (desc) {
                assertTrue(sgn > 0);
            } else {
                assertTrue(sgn < 0);
            }
        }
    }

    @Test
    public void fuzzBigDecimalKey() throws Exception {
        fuzzBigDecimalKey(false);
    }

    @Test
    public void fuzzBigDecimalKeyDesc() throws Exception {
        fuzzBigDecimalKey(true);
    }

    private void fuzzBigDecimalKey(boolean desc) throws Exception {
        var rnd = new Random(desc ? 123654 : 456321);
        var ref = new BigDecimal[1];

        BigDecimal lastValue = null;
        byte[] lastEncoded = null;

        for (int i=0; i<100_000; i++) {
            BigDecimal value;
            int mode = rnd.nextInt(20);
            if (mode == 0) {
                int len = rnd.nextInt(10 * 8);
                var unscaled = new BigInteger(len, rnd);
                if (rnd.nextBoolean()) {
                    unscaled = unscaled.negate();
                }
                int scale = rnd.nextInt(10) - 5;
                value = new BigDecimal(unscaled, scale);
            } else {
                int len = rnd.nextInt(100 * 8);
                var unscaled = new BigInteger(len, rnd);
                if (rnd.nextBoolean()) {
                    unscaled = unscaled.negate();
                }
                int scale = rnd.nextInt();
                value = new BigDecimal(unscaled, scale);
            }

            byte[] encoded;
            if (desc) {
                encoded = BigDecimalUtils.encodeBigDecimalKeyDesc(value);
            } else {
                encoded = BigDecimalUtils.encodeBigDecimalKey(value);
            }

            int offset;
            if (desc) {
                offset = BigDecimalUtils.decodeBigDecimalKeyDesc(encoded, 0, ref);
            } else {
                offset = BigDecimalUtils.decodeBigDecimalKey(encoded, 0, ref);
            }
            if (value != null && value.compareTo(BigDecimal.ZERO) == 0) {
                assertTrue(value.compareTo(ref[0]) == 0);
            } else {
                assertEquals(value, ref[0]);
            }
            assertEquals(encoded.length, offset);

            if (lastEncoded != null &&
                (value.compareTo(BigDecimal.ZERO) != 0 ||
                 lastValue.compareTo(BigDecimal.ZERO) != 0))
            {
                int sgn = Integer.signum(value.compareTo(lastValue));
                if (desc) {
                    sgn = -sgn;
                }
                assertEquals(sgn, Integer.signum(Arrays.compareUnsigned(encoded, lastEncoded)));
            }

            lastValue = value;
            lastEncoded = encoded;
        }
    }
}
