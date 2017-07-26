/*
 * Copyright (c) 2014, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

/*
 *  Copyright (C) 2017 Cojen.org
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

package org.cojen.tupl.io;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import java.util.zip.Checksum;

/**
 * Utility for computing the CRC-32C of a data stream. When used with Java 9, a potentially
 * optimized instance is used.
 *
 * @author Brian S O'Neill
 */
public class CRC32C {
    private static final MethodHandle INSTANCE_CTOR;
    private static final MethodHandle UPDATE_BYTE_BUFFER;

    static {
        MethodHandle ctor = null;
        MethodType voidType = MethodType.methodType(void.class);

        Class clazz;
        try {
            clazz = Class.forName("java.util.zip.CRC32C");
            ctor = MethodHandles.publicLookup().findConstructor(clazz, voidType);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            // Use default implementation.
            clazz = Impl.class;
        }

        if (ctor == null) {
            try {
                ctor = MethodHandles.lookup().findConstructor(clazz, voidType);
            } catch (Throwable e) {
                throw Utils.rethrow(e);
            }
        }

        INSTANCE_CTOR = ctor;

        try {
            UPDATE_BYTE_BUFFER = MethodHandles.lookup().findVirtual
                (clazz, "update", MethodType.methodType(void.class, ByteBuffer.class));
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    public static Checksum newInstance() {
        try {
            return (Checksum) INSTANCE_CTOR.invoke();
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * Updates the CRC-32C checksum with the bytes from the specified buffer.
     *
     * The checksum is updated with the remaining bytes in the buffer, starting
     * at the buffer's position. Upon return, the buffer's position will be
     * updated to its limit; its limit will not have been changed.
     */
    public static void update(Checksum crc, ByteBuffer buffer) {
        try {
            UPDATE_BYTE_BUFFER.invoke(crc, buffer);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    private CRC32C() {
    }

    static class Impl implements Checksum {

        /*
         * This CRC-32C implementation uses the 'slicing-by-8' algorithm described
         * in the paper "A Systematic Approach to Building High Performance
         * Software-Based CRC Generators" by Michael E. Kounavis and Frank L. Berry,
         * Intel Research and Development
         */

        /**
         * CRC-32C Polynomial
         */
        private static final int CRC32C_POLY = 0x1EDC6F41;
        private static final int REVERSED_CRC32C_POLY = Integer.reverse(CRC32C_POLY);

        private static final sun.misc.Unsafe UNSAFE = UnsafeAccess.tryObtain();

        private static final int ADDRESS_SIZE;
        private static final int ARRAY_BYTE_BASE_OFFSET;
        private static final int ARRAY_BYTE_INDEX_SCALE;

        // Lookup tables
        // Lookup table for single byte calculations
        private static final int[] byteTable;
        // Lookup tables for bulk operations in 'slicing-by-8' algorithm
        private static final int[][] byteTables = new int[8][256];
        private static final int[] byteTable0 = byteTables[0];
        private static final int[] byteTable1 = byteTables[1];
        private static final int[] byteTable2 = byteTables[2];
        private static final int[] byteTable3 = byteTables[3];
        private static final int[] byteTable4 = byteTables[4];
        private static final int[] byteTable5 = byteTables[5];
        private static final int[] byteTable6 = byteTables[6];
        private static final int[] byteTable7 = byteTables[7];

        static {
            ADDRESS_SIZE = UNSAFE.addressSize();
            ARRAY_BYTE_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
            ARRAY_BYTE_INDEX_SCALE = UNSAFE.arrayIndexScale(byte[].class);

            // Generate lookup tables
            // High-order polynomial term stored in LSB of r.
            for (int index = 0; index < byteTables[0].length; index++) {
                int r = index;
                for (int i = 0; i < Byte.SIZE; i++) {
                    if ((r & 1) != 0) {
                        r = (r >>> 1) ^ REVERSED_CRC32C_POLY;
                    } else {
                        r >>>= 1;
                    }
                }
                byteTables[0][index] = r;
            }

            for (int index = 0; index < byteTables[0].length; index++) {
                int r = byteTables[0][index];

                for (int k = 1; k < byteTables.length; k++) {
                    r = byteTables[0][r & 0xFF] ^ (r >>> 8);
                    byteTables[k][index] = r;
                }
            }

            if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
                byteTable = byteTables[0];
            } else { // ByteOrder.BIG_ENDIAN
                byteTable = new int[byteTable0.length];
                System.arraycopy(byteTable0, 0, byteTable, 0, byteTable0.length);
                for (int[] table : byteTables) {
                    for (int index = 0; index < table.length; index++) {
                        table[index] = Integer.reverseBytes(table[index]);
                    }
                }
            }
        }

        /**
         * Calculated CRC-32C value
         */
        private int crc = 0xFFFFFFFF;

        /**
         * Creates a new CRC32C object.
         */
        Impl() {
        }

        /**
         * Updates the CRC-32C checksum with the specified byte (the low eight bits
         * of the argument b).
         */
        @Override
        public void update(int b) {
            crc = (crc >>> 8) ^ byteTable[(crc ^ (b & 0xFF)) & 0xFF];
        }

        /**
         * Updates the CRC-32C checksum with the specified array of bytes.
         *
         * @throws ArrayIndexOutOfBoundsException
         *         if {@code off} is negative, or {@code len} is negative, or
         *         {@code off+len} is negative or greater than the length of
         *         the array {@code b}.
         */
        @Override
        public void update(byte[] b, int off, int len) {
            if (b == null) {
                throw new NullPointerException();
            }
            if (off < 0 || len < 0 || off > b.length - len) {
                throw new ArrayIndexOutOfBoundsException();
            }
            crc = updateBytes(crc, b, off, (off + len));
        }

        /**
         * Updates the CRC-32C checksum with the bytes from the specified buffer.
         *
         * The checksum is updated with the remaining bytes in the buffer, starting
         * at the buffer's position. Upon return, the buffer's position will be
         * updated to its limit; its limit will not have been changed.
         */
        public void update(ByteBuffer buffer) {
            int pos = buffer.position();
            int limit = buffer.limit();
            assert (pos <= limit);
            int rem = limit - pos;
            if (rem <= 0) {
                return;
            }

            if (buffer.isDirect()) {
                long address;
                try {
                    address = UNSAFE.getLong(buffer, DirectAccess.cDirectAddressOffset);
                    crc = updateDirectByteBuffer(crc, address, pos, limit);
                } catch (Exception e) {
                    throw new UnsupportedOperationException(e);
                }
            } else if (buffer.hasArray()) {
                crc = updateBytes(crc, buffer.array(), pos + buffer.arrayOffset(),
                                  limit + buffer.arrayOffset());
            } else {
                byte[] b = new byte[Math.min(buffer.remaining(), 4096)];
                while (buffer.hasRemaining()) {
                    int length = Math.min(buffer.remaining(), b.length);
                    buffer.get(b, 0, length);
                    update(b, 0, length);
                }
            }
            buffer.position(limit);
        }

        /**
         * Resets CRC-32C to initial value.
         */
        @Override
        public void reset() {
            crc = 0xFFFFFFFF;
        }

        /**
         * Returns CRC-32C value.
         */
        @Override
        public long getValue() {
            return (~crc) & 0xFFFFFFFFL;
        }

        /**
         * Updates the CRC-32C checksum with the specified array of bytes.
         */
        private static int updateBytes(int crc, byte[] b, int off, int end) {

            // Do only byte reads for arrays so short they can't be aligned
            // or if bytes are stored with a larger witdh than one byte.,%
            if (end - off >= 8 && ARRAY_BYTE_INDEX_SCALE == 1) {

                // align on 8 bytes
                int alignLength
                    = (8 - ((ARRAY_BYTE_BASE_OFFSET + off) & 0x7)) & 0x7;
                for (int alignEnd = off + alignLength; off < alignEnd; off++) {
                    crc = (crc >>> 8) ^ byteTable[(crc ^ b[off]) & 0xFF];
                }

                if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
                    crc = Integer.reverseBytes(crc);
                }

                // slicing-by-8
                for (; off < (end - Long.BYTES); off += Long.BYTES) {
                    int firstHalf;
                    int secondHalf;
                    if (ADDRESS_SIZE == 4) {
                        // On 32 bit platforms read two ints instead of a single 64bit long
                        firstHalf = UNSAFE.getInt(b, (long)ARRAY_BYTE_BASE_OFFSET + off);
                        secondHalf = UNSAFE.getInt(b, (long)ARRAY_BYTE_BASE_OFFSET + off
                                                   + Integer.BYTES);
                    } else {
                        long value = UNSAFE.getLong(b, (long)ARRAY_BYTE_BASE_OFFSET + off);
                        if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
                            firstHalf = (int) value;
                            secondHalf = (int) (value >>> 32);
                        } else { // ByteOrder.BIG_ENDIAN
                            firstHalf = (int) (value >>> 32);
                            secondHalf = (int) value;
                        }
                    }
                    crc ^= firstHalf;
                    if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
                        crc = byteTable7[crc & 0xFF]
                            ^ byteTable6[(crc >>> 8) & 0xFF]
                            ^ byteTable5[(crc >>> 16) & 0xFF]
                            ^ byteTable4[crc >>> 24]
                            ^ byteTable3[secondHalf & 0xFF]
                            ^ byteTable2[(secondHalf >>> 8) & 0xFF]
                            ^ byteTable1[(secondHalf >>> 16) & 0xFF]
                            ^ byteTable0[secondHalf >>> 24];
                    } else { // ByteOrder.BIG_ENDIAN
                        crc = byteTable0[secondHalf & 0xFF]
                            ^ byteTable1[(secondHalf >>> 8) & 0xFF]
                            ^ byteTable2[(secondHalf >>> 16) & 0xFF]
                            ^ byteTable3[secondHalf >>> 24]
                            ^ byteTable4[crc & 0xFF]
                            ^ byteTable5[(crc >>> 8) & 0xFF]
                            ^ byteTable6[(crc >>> 16) & 0xFF]
                            ^ byteTable7[crc >>> 24];
                    }
                }

                if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
                    crc = Integer.reverseBytes(crc);
                }
            }

            // Tail
            for (; off < end; off++) {
                crc = (crc >>> 8) ^ byteTable[(crc ^ b[off]) & 0xFF];
            }

            return crc;
        }

        /**
         * Updates the CRC-32C checksum reading from the specified address.
         */
        private static int updateDirectByteBuffer(int crc, long address,
                                                  int off, int end) {

            // Do only byte reads for arrays so short they can't be aligned
            if (end - off >= 8) {

                // align on 8 bytes
                int alignLength = (8 - (int) ((address + off) & 0x7)) & 0x7;
                for (int alignEnd = off + alignLength; off < alignEnd; off++) {
                    crc = (crc >>> 8)
                        ^ byteTable[(crc ^ UNSAFE.getByte(address + off)) & 0xFF];
                }

                if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
                    crc = Integer.reverseBytes(crc);
                }

                // slicing-by-8
                for (; off <= (end - Long.BYTES); off += Long.BYTES) {
                    // Always reading two ints as reading a long followed by
                    // shifting and casting was slower.
                    int firstHalf = UNSAFE.getInt(address + off);
                    int secondHalf = UNSAFE.getInt(address + off + Integer.BYTES);
                    crc ^= firstHalf;
                    if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
                        crc = byteTable7[crc & 0xFF]
                            ^ byteTable6[(crc >>> 8) & 0xFF]
                            ^ byteTable5[(crc >>> 16) & 0xFF]
                            ^ byteTable4[crc >>> 24]
                            ^ byteTable3[secondHalf & 0xFF]
                            ^ byteTable2[(secondHalf >>> 8) & 0xFF]
                            ^ byteTable1[(secondHalf >>> 16) & 0xFF]
                            ^ byteTable0[secondHalf >>> 24];
                    } else { // ByteOrder.BIG_ENDIAN
                        crc = byteTable0[secondHalf & 0xFF]
                            ^ byteTable1[(secondHalf >>> 8) & 0xFF]
                            ^ byteTable2[(secondHalf >>> 16) & 0xFF]
                            ^ byteTable3[secondHalf >>> 24]
                            ^ byteTable4[crc & 0xFF]
                            ^ byteTable5[(crc >>> 8) & 0xFF]
                            ^ byteTable6[(crc >>> 16) & 0xFF]
                            ^ byteTable7[crc >>> 24];
                    }
                }

                if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
                    crc = Integer.reverseBytes(crc);
                }
            }

            // Tail
            for (; off < end; off++) {
                crc = (crc >>> 8)
                    ^ byteTable[(crc ^ UNSAFE.getByte(address + off)) & 0xFF];
            }

            return crc;
        }
    }
}
