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

package org.cojen.tupl.io;

import static org.junit.Assume.*;
import static org.junit.Assert.*;
import static org.cojen.tupl.TestUtils.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.EnumSet;

import org.junit.*;

//import com.sun.jna.Platform;

public class FileIOTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(FileIOTest.class.getName());
    }

    private File file;

    @Before
    public void setup() throws Exception {
        file = newTempBaseFile(getClass());
    }

    @After
    public void teardown() throws Exception {
        if (file != null && file.exists()) {
            file.delete();
        }
    }

    // Newer versions of Linux allocate as much as possible instead of failing atomically. The
    // FileIO class reverts the allocation, but until it finishes, other threads and processes
    // writing to the temp directory fail with "No space left on device".
    //@Ignore
    //@Test
    /*
    public void preallocateTooLarge() throws Exception {
        assumeTrue(Platform.isLinux());
        try (FileIO fio = FileIO.open(file, EnumSet.of(OpenOption.CREATE, OpenOption.MAPPED))) {
            FileStore fs = Files.getFileStore(file.toPath());
            assumeTrue("ext4".equals(fs.type()) && !fs.name().contains("docker"));

            long len = file.getTotalSpace() * 100L;
            // setLength traps the IOException and prevents resizing / remapping. Not remapping
            // avoids the SIGBUS issue.
            fio.expandLength(len, LengthOption.PREALLOCATE_ALWAYS);
            assertEquals(0, fio.length());
        }
    }
    */

    @Test
    public void preallocateGrowShrink() throws Exception {
        try (FileIO fio = FileIO.open(file, EnumSet.of(OpenOption.CREATE))) {
            for (long len = 0; len <= 50_000L; len += 5000L) {
                fio.expandLength(len, LengthOption.PREALLOCATE_ALWAYS);
                assertEquals(len, fio.length());
            }
            for (long len = 50_000L; len >= 0; len -= 5000L) {
                fio.truncateLength(len);
                assertEquals(len, fio.length());
            }
        }
    }

    //@Ignore
    //@Test
    /*
    public void preallocate() throws Exception {
        try {
            doPreallocate();
        } catch (AssertionError e) {
            // Try again in case any external files messed up the test.
            teardown();
            setup();
            doPreallocate();
        }
    }

    private void doPreallocate() throws Exception {
        // Assuming a filesystem with delayed block allocation, e.g. ext3 / ext4 on Linux.
        // Test on Windows too, to test the default doPreallocate method.
        assumeTrue(Platform.isLinux() || Platform.isMac() || Platform.isWindows());

        long len = 100L * (1<<20); // 100 MB
        try (FileIO fio = FileIO.open(file, EnumSet.of(OpenOption.CREATE))) {
            long startFree = file.getFreeSpace();

            fio.expandLength(len, LengthOption.PREALLOCATE_ALWAYS);
            long alloc = startFree - file.getFreeSpace();

            // Free space should be reduced. Pad expectation since external events may interfere.
            assertTrue(alloc > (len >> 1));
        }
    }
    */

    //@Ignore
    //@Test
    /*
    public void crash() throws Exception {
        // Reproduces SIGBUS crash. Requires writing direct buffers
        // to a mapped file when there is not enough space left. To avoid having
        // to fill up a primary device use a small ramdisk, or loop device. E.g. 
        //   $ dd if=/dev/zero of=file.img bs=1M count=100 
        //   $ losetup /dev/loop0 file.img
        //   $ mkfs.ext4 /dev/loop0
        //   $ mkdir -p root
        //   $ mount -t ext4 /dev/loop0 root
        
        assumeTrue(Platform.isLinux()); 
        var f = new File("root/test.file");
        if (f.exists()) f.delete();

        FileIO fio = FileIO.open(f, EnumSet.of(OpenOption.CREATE));
        final long len = f.getTotalSpace() * 5L;
        fio.expandLength(len, LengthOption.PREALLOCATE_ALWAYS);
        fio.map();

        ByteBuffer bb = ByteBuffer.allocateDirect(4097);
        for (long pos = 0; pos < len - bb.remaining(); pos += 512) {
            fio.write(pos, bb);
            bb.flip();
        }

        fio.close();
    }
    */
}
