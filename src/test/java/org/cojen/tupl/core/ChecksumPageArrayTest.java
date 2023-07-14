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

package org.cojen.tupl.core;

import java.io.File;

import java.util.EnumSet;

import java.util.zip.CRC32C;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.TestUtils;

import org.cojen.tupl.io.FilePageArray;
import org.cojen.tupl.io.OpenOption;
import org.cojen.tupl.io.PageArray;
import org.cojen.tupl.io.Utils;

import org.cojen.tupl.unsafe.DirectAccess;
import org.cojen.tupl.unsafe.UnsafeAccess;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ChecksumPageArrayTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ChecksumPageArrayTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mFile = TestUtils.newTempBaseFile(getClass());
    }

    @After
    public void teardown() throws Exception {
        if (mSource != null) {
            mSource.close();
        }
        TestUtils.deleteTempFiles(getClass());
    }

    private File mFile;
    private PageArray mSource;

    @Test
    public void basic() throws Exception {
        basic(false);
    }

    private void basic(boolean directIO) throws Exception {
        var options = EnumSet.of(OpenOption.CREATE);

        if (directIO) {
            options.add(OpenOption.DIRECT_IO);
        }

        mSource = new FilePageArray(512, mFile, options);

        PageArray pa = ChecksumPageArray.open(mSource, CRC32C::new);
        int pageSize = 512 - 4;
        assertEquals(pageSize, pa.pageSize());

        int physicalPageSize = pageSize;

        if (pa.isDirectIO()) {
            // Page sizes must match.
            physicalPageSize += 4;
        }

        int writeOffset = 10;
        var writePage = new byte[writeOffset + physicalPageSize];
        for (int i=0; i<writePage.length; i++) {
            writePage[i] = (byte) i;
        }

        pa.writePage(1, writePage, writeOffset);

        int readOffset = 20;
        var readPage = new byte[readOffset + physicalPageSize];
        pa.readPage(1, readPage, readOffset, physicalPageSize);

        for (int i=0; i<pageSize; i++) {
            assertEquals(writePage[writeOffset + i], readPage[readOffset + i]);
        }

        var srcPage = new byte[pageSize + 4];
        mSource.readPage(1, srcPage);

        for (int i=0; i<pageSize; i++) {
            assertEquals(srcPage[i], readPage[readOffset + i]);
        }

        var crc = new CRC32C();
        crc.update(writePage, writeOffset, pageSize);
        int expectedCrc = (int) crc.getValue();

        int actualCrc = Utils.decodeIntLE(srcPage, pageSize);

        assertEquals(expectedCrc, actualCrc);
    }

    @Test
    public void basicPtrs() throws Exception {
        basicPtrs(false);
    }

    private void basicPtrs(boolean directIO) throws Exception {
        var options = EnumSet.of(OpenOption.CREATE);

        if (directIO) {
            options.add(OpenOption.DIRECT_IO);
        }

        mSource = new FilePageArray(4096, mFile, options);

        PageArray pa = ChecksumPageArray.open(mSource, CRC32C::new);
        int pageSize = 4096 - 4;
        assertEquals(pageSize, pa.pageSize());

        int physicalPageSize = pageSize;

        if (pa.isDirectIO()) {
            // Page sizes must match.
            physicalPageSize += 4;
        }

        int writeOffset = directIO ? 0 : 15;
        int writePageSize = writeOffset + physicalPageSize;
        long writePagePtr = alloc(writePageSize, pa);
        for (int i=0; i<writePageSize; i++) {
            DirectPageOps.p_bytePut(writePagePtr, i, i);
        }

        pa.writePage(2, writePagePtr, writeOffset);

        int readOffset = 25;
        int readPageSize = readOffset + physicalPageSize;
        long readPagePtr = alloc(readPageSize, pa);
        pa.readPage(2, readPagePtr, readOffset, physicalPageSize);

        for (int i=0; i<pageSize; i++) {
            assertEquals(DirectPageOps.p_byteGet(writePagePtr, writeOffset + i),
                         DirectPageOps.p_byteGet(readPagePtr, readOffset + i));
        }

        var srcPage = new byte[pageSize + 4];
        mSource.readPage(2, srcPage);

        for (int i=0; i<pageSize; i++) {
            assertEquals(srcPage[i], DirectPageOps.p_byteGet(readPagePtr, readOffset + i));
        }

        var crc = new CRC32C();
        crc.update(DirectAccess.ref(writePagePtr + writeOffset, pageSize));
        int expectedCrc = (int) crc.getValue();

        int actualCrc = Utils.decodeIntLE(srcPage, pageSize);

        assertEquals(expectedCrc, actualCrc);

        UnsafeAccess.free(writePagePtr);
        UnsafeAccess.free(readPagePtr);
    }

    private static long alloc(int size, PageArray pa) {
        return UnsafeAccess.alloc(size, pa.isDirectIO());
    }
}
