/*
 *  Copyright 2012-2016 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl;

import static org.junit.Assume.*;
import static org.junit.Assert.*;
import static org.cojen.tupl.TestUtils.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.EnumSet;

import org.cojen.tupl.io.FileIO;
import org.cojen.tupl.io.OpenOption;
import org.junit.*;

import com.sun.jna.Platform;

public class FileIOTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(FileIOTest.class.getName());
    }

    private File file;

    @Before
    public void setup() throws Exception {
        file = newTempBaseFile();
    }

    @After
    public void teardown() throws Exception {
        if (file != null && file.exists()) {
            file.delete();
        }
    }

    @Test
    public void preallocateTooLarge() throws Exception {
        assumeTrue(Platform.isLinux()); 
        FileIO fio = FileIO.open(file, EnumSet.of(OpenOption.CREATE, OpenOption.MAPPED, OpenOption.PREALLOCATE));

        FileStore fs = Files.getFileStore(file.toPath());
        assumeTrue("ext4".equals(fs.type()) && !fs.name().contains("docker"));

        long len = file.getTotalSpace() * 100L; 
        // setLength traps the IOException and prevents resizing / remapping. Not remapping avoids
        // the SIGBUS issue.
        fio.setLength(len);
        assertEquals(0, fio.length());

        fio.close();
    }

    @Test
    public void preallocateGrowShrink() throws Exception {
        FileIO fio = FileIO.open(file, EnumSet.of(OpenOption.CREATE, OpenOption.PREALLOCATE));
        for (long len = 0; len <= 50_000L; len += 5000L) {
            fio.setLength(len);
            assertEquals(len, fio.length());
        }
        for (long len = 50_000L; len >= 0; len -= 5000L) {
            fio.setLength(len);
            assertEquals(len, fio.length());
        }

        fio.close();
    }

    @Test
    public void preallocate() throws Exception {
        // Assuming a filesystem with delayed block allocation,
        // e.g. ext3 / ext4 on Linux.
        assumeTrue(Platform.isLinux() || Platform.isMac());

        long len = 100L * (1<<20); // 100 MB
        FileIO fio = FileIO.open(file, EnumSet.of(OpenOption.CREATE, OpenOption.PREALLOCATE));
        long startFree = file.getFreeSpace();

        fio.setLength(len);
        long alloc = startFree - file.getFreeSpace();
                        
        // Free space should be reduced. Pad expectation since external
        // events may interfere.
        assertTrue(alloc > (len >> 1));

        fio.close();
    }

    @Ignore
    @Test
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
        File f = new File("root/test.file");
        if (f.exists()) f.delete();

        FileIO fio = FileIO.open(f, EnumSet.of(OpenOption.CREATE, OpenOption.PREALLOCATE));
        final long len = f.getTotalSpace() * 5L;
        fio.setLength(len);
        fio.map();

        ByteBuffer bb = ByteBuffer.allocateDirect(4097);
        for (long pos = 0; pos < len - bb.remaining(); pos += 512) {
            fio.write(pos, bb);
            bb.flip();
        }

        fio.close();
    }
}
