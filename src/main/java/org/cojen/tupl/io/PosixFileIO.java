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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import java.nio.ByteBuffer;

import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class PosixFileIO extends AbstractFileIO {
    static {
        /*
          From the JNA documentation: Direct mapping supports the same type mappings as
          interface mapping, except for arrays of Pointer/Structure/String/WString/NativeMapped
          as function arguments. In addition, direct mapping does not support NIO Buffers or
          primitive arrays as types returned by type mappers or NativeMapped.
          Also: varargs isn't supported
         */
        Native.register(Platform.C_LIBRARY_NAME);
    }

    private static final int REOPEN_NON_DURABLE = 1, REOPEN_SYNC_IO = 2, REOPEN_DIRECT_IO = 4;

    private final File mFile;
    private final int mReopenOptions;

    private final ThreadLocal<BufRef> mBufRef;
    private final boolean mReadahead;
    private final boolean mCloseDontNeed;

    private int mFileDescriptor;

    PosixFileIO(File file, EnumSet<OpenOption> options) throws IOException {
        super(options);

        mFile = file;

        if (options.contains(OpenOption.NON_DURABLE)) {
            mReopenOptions = REOPEN_NON_DURABLE;
        } else {
            int reopenOpts = 0;
            if (options.contains(OpenOption.SYNC_IO)) {
                reopenOpts |= REOPEN_SYNC_IO;
            }
            if (options.contains(OpenOption.DIRECT_IO)) {
                reopenOpts |= REOPEN_DIRECT_IO;
            }
            mReopenOptions = reopenOpts;
            if (options.contains(OpenOption.CREATE)) {
                new JavaFileIO(file, options, 1, false).close();
            }
        }
        mReadahead = options.contains(OpenOption.READAHEAD);
        mCloseDontNeed = options.contains(OpenOption.CLOSE_DONTNEED);

        mAccessLock.acquireExclusive();
        try {
            mFileDescriptor = openFd(file, options);
        } finally {
            mAccessLock.releaseExclusive();
        }

        mBufRef = new ThreadLocal<>();

        if (options.contains(OpenOption.MAPPED)) {
            map();
        }

        if (options.contains(OpenOption.CREATE)) {
            dirSync(file);
        }
    }

    @Override
    public boolean isDirectIO() {
        return (mReopenOptions & REOPEN_DIRECT_IO) != 0;
    }

    @Override
    protected long doLength() throws IOException {
        return lseekEndFd(fd(), 0);
    }

    @Override
    protected void doSetLength(long length) throws IOException {
        ftruncateFd(fd(), length);
    }

    @Override
    protected void doRead(long pos, byte[] buf, int offset, int length) throws IOException {
        BufRef ref = bufRef(length);
        doRead(pos, ref.mPointer, length);
        ByteBuffer bb = ref.mBuffer;
        bb.position(0);
        bb.get(buf, offset, length);
    }

    @Override
    protected void doRead(long pos, ByteBuffer bb) throws IOException {
        int bufPos = bb.position();
        int bufLen = bb.limit() - bufPos;
        if (bb.isDirect()) {
            doRead(pos, DirectAccess.getAddress(bb) + bufPos, bufLen);
        } else {
            doRead(pos, (byte[]) bb.array(), bb.arrayOffset() + bufPos, bufLen);
        }
        bb.position(bb.limit());
    }

    @Override
    protected void doRead(long pos, long ptr, int length) throws IOException {
        preadFd(fd(), ptr, length, pos);
    }

    @Override
    protected void doWrite(long pos, byte[] buf, int offset, int length) throws IOException {
        BufRef ref = bufRef(length);
        ByteBuffer bb = ref.mBuffer;
        bb.position(0);
        bb.put(buf, offset, length);
        doWrite(pos, ref.mPointer, length);
    }

    @Override
    protected void doWrite(long pos, ByteBuffer bb) throws IOException {
        int bufPos = bb.position();
        int bufLen = bb.limit() - bufPos;
        if (bb.isDirect()) {
            doWrite(pos, DirectAccess.getAddress(bb) + bufPos, bufLen);
        } else {
            doWrite(pos, (byte[]) bb.array(), bb.arrayOffset() + bufPos, bufLen);
        }
        bb.position(bb.limit());
    }

    @Override
    protected void doWrite(long pos, long ptr, int length) throws IOException {
        pwriteFd(fd(), ptr, length, pos);
    }

    @Override
    protected Mapping openMapping(boolean readOnly, long pos, int size) throws IOException {
        if (mReadahead) {
            // Apply readahead only when this file is mapped to prevent unnecessary memory churn.
            fadvise(mFileDescriptor, pos, size, 3); // 3 = POSIX_FADV_WILLNEED
        }
        return new PosixMapping(mFileDescriptor, readOnly, pos, size);
    }

    @Override
    protected void reopen() throws IOException {
        closeFd(fd());

        EnumSet<OpenOption> options = EnumSet.noneOf(OpenOption.class);
        if (isReadOnly()) {
            options.add(OpenOption.READ_ONLY);
        }
        if ((mReopenOptions & REOPEN_SYNC_IO) != 0) {
            options.add(OpenOption.SYNC_IO);
        }
        if ((mReopenOptions & REOPEN_NON_DURABLE) != 0) {
            options.add(OpenOption.NON_DURABLE);
        }
        if ((mReopenOptions & REOPEN_DIRECT_IO) != 0) {
            options.add(OpenOption.DIRECT_IO);
        }

        mFileDescriptor = openFd(mFile, options);
    }

    @Override
    protected void doSync(boolean metadata) throws IOException {
        int fd = fd();
        if (metadata) {
            fsyncFd(fd);
        } else {
            fdatasyncFd(fd);
        }
    }

    @Override
    public void close(Throwable cause) throws IOException {
        int fd;

        mAccessLock.acquireExclusive();
        try {
            fd = mFileDescriptor;
            if (fd == 0) {
                return;
            }
            mCause = cause;
            mFileDescriptor = 0;
        } finally {
            mAccessLock.releaseExclusive();
        }

        IOException ex = null;
        try {
            unmap(false);
        } catch (IOException e) {
            ex = e;
        }

        if (mCloseDontNeed) {
            // Hint to the kernel that it can release pages associated with this
            // file. It is free to ignore our advice, but generally helps
            // prevent filling up the page cache with useless data. On numa
            // machines page cache pollution can cause unnecesary trashing.
            
            // Using length of 0 means to apply the hint from the offset to EOF.
            fadvise(fd, 0, 0, 4); // 4 = POSIX_FADV_DONTNEED
        }

        try {
            closeFd(fd);
        } catch (IOException e) {
            Utils.suppress(e, ex);
            throw e;
        }
    }

    private BufRef bufRef(int size) {
        BufRef ref = mBufRef.get();
        if (ref == null || ref.mBuffer.capacity() < size) {
            ref = new BufRef(ByteBuffer.allocateDirect(size));
            mBufRef.set(ref);
        }
        return ref;
    }

    // Caller must hold mAccessLock.
    private int fd() throws IOException {
        int fd = mFileDescriptor;
        if (fd == 0) {
            IOException ex = new ClosedChannelException();
            ex.initCause(mCause);
            throw ex;
        }
        return fd;
    }

    /**
     * @return fd
     */
    static int openFd(File file, EnumSet<OpenOption> options) throws IOException {
        // Select O_RDONLY or O_RDWR.
        int flags = 0;
        if (!options.contains(OpenOption.READ_ONLY)) {
            flags |= 2;
        }

        int fd;

        if (options.contains(OpenOption.NON_DURABLE)) {
            if (options.contains(OpenOption.CREATE)) {
                flags |= 0100; // O_CREAT
            }
            int mode = 0600;
            fd = RT.shm_open(file.getPath(), flags, mode);
        } else {
            if (options.contains(OpenOption.SYNC_IO)) {
                flags |= 010000;
            }
            if (options.contains(OpenOption.DIRECT_IO)) {
                flags |= 040000;
            }
            fd = open(file.getPath(), flags);
        }

        if (fd == -1) {
            throw lastErrorToException();
        }

        if (options.contains(OpenOption.RANDOM_ACCESS)) {
            try {
                fadvise(fd, 0, 0, 1); // 1 = POSIX_FADV_RANDOM
            } catch (Throwable e) {
                try {
                    closeFd(fd);
                } catch (IOException e2) {
                    Utils.suppress(e, e2);
                }
                throw e;
            }
        }

        return fd;
    }

    static long lseekSetFd(int fd, long fileOffset) throws IOException {
        return lseekFd(fd, fileOffset, 0); // SEEK_SET
    }

    static long lseekCurFd(int fd, long fileOffset) throws IOException {
        return lseekFd(fd, fileOffset, 1); // SEEK_CUR
    }

    static long lseekEndFd(int fd, long fileOffset) throws IOException {
        return lseekFd(fd, fileOffset, 2); // SEEK_END
    }

    static long lseekFd(int fd, long fileOffset, int whence) throws IOException {
        long result = lseek(fd, fileOffset, whence);
        if (result == -1) {
            throw lastErrorToException();
        }
        return result;
    }

    static void preadFd(int fd, long bufPtr, int length, long fileOffset) throws IOException {
        while (true) {
            int amt = pread(fd, bufPtr, length, fileOffset);
            if (amt <= 0) {
                if (amt < 0) {
                    throw lastErrorToException();
                }
                if (length > 0) {
                    throw new EOFException("Attempt to read past end of file: " + fileOffset);
                }
                return;
            }
            length -= amt;
            if (length <= 0) {
                return;
            }
            bufPtr += amt;
            fileOffset += amt;
        }
    }

    static void pwriteFd(int fd, long bufPtr, int length, long fileOffset) throws IOException {
        while (true) {
            int amt = pwrite(fd, bufPtr, length, fileOffset);
            if (amt < 0) {
                throw lastErrorToException();
            }
            length -= amt;
            if (length <= 0) {
                return;
            }
            bufPtr += amt;
            fileOffset += amt;
        }
    }

    static void ftruncateFd(int fd, long length) throws IOException {
        if (ftruncate(fd, length) == -1) {
            throw lastErrorToException();
        }
    }

    static void fsyncFd(int fd) throws IOException {
        int result;
        if (Platform.isMac()) {
            result = fcntl(fd, 51); // F_FULLFSYNC
        } else {
            result = fsync(fd);
        }
        if (result == -1) {
            throw lastErrorToException();
        }
    }

    static void fdatasyncFd(int fd) throws IOException {
        int result;
        if (Platform.isMac()) {
            result = fcntl(fd, 51); // F_FULLFSYNC
        } else {
            result = fdatasync(fd);
        }
        if (result == -1) {
            throw lastErrorToException();
        }
    }

    static void fadvise(int fd, long offset, long length, int advice) throws IOException {
        int result = platform().fadvise(fd, offset, length, advice);
        if (result != 0) {
            throw new IOException(errorMessage(result));
        }
    }

    static void closeFd(int fd) throws IOException {
        if (close(fd) == -1) {
            throw lastErrorToException();
        }
    }

    static long mmapFd(long length, int prot, int flags, int fd, long offset) throws IOException {
        long ptr = mmap(0, length, prot, flags, fd, offset);
        if (ptr == -1) {
            throw lastErrorToException();
        }
        return ptr;
    }

    static void msyncAddr(long addr, long length) throws IOException {
        if (msync(addr, length, 4) == -1) { // flags = MS_SYNC
            throw lastErrorToException();
        }
    }

    static void munmapAddr(long addr, long length) throws IOException {
        if (munmap(addr, length) == -1) {
            throw lastErrorToException();
        }
    }

    static IOException lastErrorToException() {
        return new IOException(errorMessage(Native.getLastError()));
    }

    static String errorMessage(int errnum) {
        final int bufLen = 200;
        long bufPtr = Native.malloc(bufLen);

        if (bufPtr != 0) {
            try {
                long result = strerror_r(errnum, bufPtr, bufLen);
                if (result != -1 && result != 22 && result != 34) { // !EINVAL && !ERANGE
                    return new Pointer(result == 0 ? bufPtr : result).getString(0);
                }
            } finally {
                Native.free(bufPtr);
            }
        }

        return "Error " + errnum;
    }

    static class BufRef {
        final ByteBuffer mBuffer;
        final long mPointer;

        BufRef(ByteBuffer buffer) {
            mBuffer = buffer;
            mPointer = Pointer.nativeValue(Native.getDirectBufferPointer(buffer));
        }
    }

    @Override
    protected boolean shouldPreallocate(LengthOption option) {
        return option == LengthOption.PREALLOCATE_ALWAYS
            || (option == LengthOption.PREALLOCATE_OPTIONAL && platform() != NullIO.INSTANCE);
    }

    @Override
    protected void doPreallocate(long pos, long length) throws IOException {
        PlatformIO platform = platform();
        if (platform == NullIO.INSTANCE) {
            // Don't have fallocate (or equivalent). Use default non-destructive zero-fill behavior.
            super.doPreallocate(pos, length);
            return;
        } 

        // Uses posix_fallocate call to quickly allocate blocks and mark them as uninitialized. 
        // If the filesystem supports fallocate then this requires no I/O to the data blocks, 
        // and is much faster than zero-filling the file, which is the fallback behavior. 
        // Since linux 2.6.31 fallocate is supported by at least btrfs, ext4, ocfs2, and 
        // xfs filesystems. Ext4 on Linux 4.2.0 takes ~30 microseconds to fallocate 64MB,
        // compared to 27 milliseconds to zero-fill that same amount.
        //
        // On OSX, uses fcntl with command F_PREALLOCATE.
        int result = platform.fallocate(fd(), pos, length);
        if (result != 0) {
            // Note: the native call above does not set errno.
            throw new IOException(errorMessage(result));
        }
    }

    /** Platform specific helper. */
    private static abstract class PlatformIO {
        public abstract int fallocate(int fd, long pos, long length);
        public abstract int fadvise(int fd, long offset, long length, int advice);
    }

    /** No-op helper. */
    private static class NullIO extends PlatformIO {
        static final NullIO INSTANCE = new NullIO();

        @Override
        public int fallocate(int fd, long pos, long length) {
            return 0;
        }

        @Override
        public int fadvise(int fd, long offset, long length, int advice) {
            return 0;
        }
    }

    /** Default POSIX I/O calls. */
    private static class DefaultIO extends PlatformIO {
        static {
            Native.register(Platform.C_LIBRARY_NAME);
        }

        @Override
        public int fallocate(int fd, long pos, long length) {
            return posix_fallocate(fd, pos, length);
        }

        @Override
        public int fadvise(int fd, long offset, long length, int advice) {
            return posix_fadvise(fd, offset, length, advice);
        }

        static native int posix_fallocate(int fd, long offset, long len);

        static native int posix_fadvise(int fd, long offset, long length, int advice);
    }

    /** 
     * Mac OSX specific I/O calls.
     *
     * For fallocate uses fcntl with the F_PREALLOCATE command to force block
     * allocation on OSX.  On a Core i5 MacBook Pro w/SSD it takes on average
     * 1.5ms to preallocate a 64MB file.
     *
     * Direct maps fcntl again with an explicit fstore_t parameter to avoid the more complex
     * and slow method of using jna varags through library mapping.
     *
     * The fadvise call is a no-op.
     */
    private static class MacIO extends PlatformIO {
        @SuppressWarnings("unused")
        public static class Fstore extends Structure {
            public static class ByReference extends Fstore implements Structure.ByReference { }

            public int  fst_flags;
            public int  fst_posmode;
            public long fst_offset;
            public long fst_length;
            public long fst_bytesalloc;

            @Override
            protected List<String> getFieldOrder() {
                return Arrays.asList(FIELDS);
            }

            private static final String[] FIELDS = new String[] {
                 "fst_flags"
                ,"fst_posmode"
                ,"fst_offset"
                ,"fst_length"
                ,"fst_bytesalloc"
            };
        }
        
        @Override
        public int fallocate(int fd, long pos, long length) {
            final Fstore.ByReference fstore = new Fstore.ByReference();
            fstore.fst_flags   = 4;   // F_ALLOCATEALL - allocate all requested space or none at all.
            fstore.fst_posmode = 3;   // F_PEOFPOSMODE
            fstore.fst_offset  = 0;
            fstore.fst_length  = length;

            int cmd = 42; // F_PREALLOCATE command
            int result = fcntl(fd, cmd, fstore);
            if (result == -1) {
                // Return errno to keep same behavior as posix_fallocate.
                return Native.getLastError();
            }
            return 0;
        }

        @Override
        public int fadvise(int fd, long offset, long length, int advice) {
            // Unsupported on OSX. 
            return 0;
        }

        static {
            Native.register(Platform.C_LIBRARY_NAME);
        }

        static native int fcntl(int fd, int cmd, Fstore.ByReference fstore);
    }


    /** Accounts for OSX not supporting some I/O operations. */
    public static class PlatformHolder {
        public static final PlatformIO INSTANCE;
        static {
            PlatformIO inst = null;
            if (Platform.isMac()) {
                inst = new MacIO();
            } else {
                try {
                    inst = new DefaultIO();
                } catch (UnsatisfiedLinkError e) {
                    inst = NullIO.INSTANCE;
                }
            }
            INSTANCE = inst;
        }
    }

    private static PlatformIO platform() {
        return PlatformHolder.INSTANCE;
    }

    static native long strerror_r(int errnum, long bufPtr, int buflen);

    static native int open(String path, int oflag);

    static native long lseek(int fd, long fileOffset, int whence);

    static native int pread(int fd, long bufPtr, int length, long fileOffset);

    static native int pwrite(int fd, long bufPtr, int length, long fileOffset);

    static native int ftruncate(int fd, long length);

    static native int fcntl(int fd, int cmd);

    static native int fsync(int fd);

    static native int fdatasync(int fd);

    static native int close(int fd);

    static native long mmap(long addr, long length, int prot, int flags, int fd, long offset);

    static native int msync(long addr, long length, int flags);

    static native int munmap(long addr, long length);

    static class RT {
        static {
            Native.register("rt");
        }

        static native int shm_open(String path, int oflag, int mode);
    }
}
