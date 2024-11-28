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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import java.util.EnumSet;

import java.util.zip.CRC32C;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.TestUtils;

import org.cojen.tupl.io.FilePageArray;
import org.cojen.tupl.io.OpenOption;
import org.cojen.tupl.io.PageArray;
import org.cojen.tupl.io.Utils;

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

    @Test
    public void basicDirect() throws Exception {
        basic(true);
    }

    private void basic(boolean directIO) throws Exception {
        var options = EnumSet.of(OpenOption.CREATE);

        if (directIO) {
            options.add(OpenOption.DIRECT_IO);
        }

        mSource = FilePageArray.factory(4096, mFile, options).get();

        PageArray pa = ChecksumPageArray.open(mSource, CRC32C::new);
        int pageSize = 4096 - 4;
        assertEquals(pageSize, pa.pageSize());

        int physicalPageSize = pageSize;

        if (pa.isDirectIO()) {
            // Page sizes must match.
            physicalPageSize += 4;
        }

        try (Arena a = Arena.ofConfined()) {
            int writeOffset = directIO ? 0 : 15;
            MemorySegment writePage = a.allocate
                (writeOffset + physicalPageSize, SysInfo.pageSize());
            for (int i=0; i<writePage.byteSize(); i++) {
                writePage.set(ValueLayout.JAVA_BYTE, i, (byte) i);
            }

            pa.writePage(2, writePage.address(), writeOffset);

            int readOffset = 25;
            MemorySegment readPage = a.allocate(readOffset + physicalPageSize, SysInfo.pageSize());
            pa.readPage(2, readPage.address(), readOffset, physicalPageSize);

            for (int i=0; i<pageSize; i++) {
                assertEquals(writePage.get(ValueLayout.JAVA_BYTE, writeOffset + i),
                             readPage.get(ValueLayout.JAVA_BYTE, readOffset + i));
            }

            MemorySegment srcPage = a.allocate(pageSize + 4, SysInfo.pageSize());
            mSource.readPage(2, srcPage.address());

            for (int i=0; i<pageSize; i++) {
                assertEquals(srcPage.get(ValueLayout.JAVA_BYTE, i),
                             readPage.get(ValueLayout.JAVA_BYTE, readOffset + i));
            }

            var crc = new CRC32C();
            crc.update(writePage.asByteBuffer()
                       .position(writeOffset).limit(writeOffset + pageSize));
            int expectedCrc = (int) crc.getValue();

            int actualCrc = srcPage.get(ValueLayout.JAVA_INT, pageSize);

            assertEquals(expectedCrc, actualCrc);
        }
    }
}
