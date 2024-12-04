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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.lang.foreign.MemorySegment;

import java.nio.ByteBuffer;

import java.nio.channels.FileChannel;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;

import java.util.EnumSet;

import java.util.function.Consumer;

import org.cojen.tupl.WriteFailureException;

import org.cojen.tupl.util.LocalPool;

/**
 * Basic FileIO implementation which uses the Java RandomAccessFile class,
 * unless a more suitable implementation is available.
 *
 * @author Brian S O'Neill
 */
sealed class JavaFileIO extends AbstractFileIO permits WindowsFileIO {
    protected final File mFile;
    private final String mMode;

    private LocalPool<FileAccess> mFilePool;

    JavaFileIO(File file, EnumSet<OpenOption> options, int openFileCount) throws IOException {
        this(file, options, openFileCount, true);
    }

    JavaFileIO(File file, EnumSet<OpenOption> options, int openFileCount, boolean allowMap)
        throws IOException
    {
        super(options);

        if (options.contains(OpenOption.NON_DURABLE)) {
            throw new UnsupportedOperationException("Unsupported options: " + options);
        }

        mFile = file;

        String mode;
        if (isReadOnly()) {
            mode = "r";
        } else {
            if (!options.contains(OpenOption.CREATE) && !file.exists()) {
                throw new FileNotFoundException(file.getPath());
            }
            if (options.contains(OpenOption.SYNC_IO)) {
                mode = "rwd";
            } else {
                mode = "rw";
            }
        }

        mMode = mode;

        if (openFileCount == 0) {
            openFileCount = 1;
        }

        mFilePool = new LocalPool<>(() -> {
            try {
                return openRaf();
            } catch (IOException e) {
                throw Utils.rethrow(e);
            }
        }, openFileCount);

        // Check that the file can actually be opened.
        testPool();

        if (allowMap && options.contains(OpenOption.MAPPED)) {
            map();
        }

        if (options.contains(OpenOption.CREATE)) {
            dirSync(file);
        }
    }

    @Override
    public final boolean isDirectIO() {
        return false;
    }

    @Override
    protected final File file() {
        return mFile;
    }

    @Override
    protected final long doLength() throws IOException {
        LocalPool.Entry<FileAccess> e = accessFile();
        try {
            return e.get().length();
        } finally {
            e.release();
        }
    }

    @Override
    protected final void doSetLength(long length) throws IOException {
        LocalPool.Entry<FileAccess> entry = accessFile();
        try {
            entry.get().setLength(length);
        } finally {
            entry.release();
        }
    }

    @Override
    protected final void doRead(long pos, byte[] buf, int offset, int length) throws IOException {
        try {
            LocalPool.Entry<FileAccess> entry = accessFile();
            try {
                FileAccess file = entry.get();
                file.seek(pos);
                file.readFully(buf, offset, length);
            } finally {
                entry.release();
            }
        } catch (EOFException e) {
            throw new EOFException("Attempt to read past end of file: " + pos);
        }
    }

    @Override
    protected final void doRead(long pos, long addr, int length) throws IOException {
        ByteBuffer bb = MemorySegment.ofAddress(addr).reinterpret(length).asByteBuffer();

        boolean interrupted = false;

        LocalPool.Entry<FileAccess> entry = accessFile();
        try {
            FileAccess file = entry.get();
            while (true) try {
                FileChannel channel = file.getChannel();
                while (bb.hasRemaining()) {
                    int amt = channel.read(bb, pos);
                    if (amt < 0) {
                        throw new EOFException("Attempt to read past end of file: " + pos);
                    }
                    pos += amt;
                }
                break;
            } catch (ClosedByInterruptException e) {
                interrupted = true;
                file = replaceClosedRaf(entry);
            }
        } finally {
            entry.release();
        }

        if (interrupted) {
            // Restore the interrupt status.
            Thread.currentThread().interrupt();
        }
    }

    @Override
    protected final void doWrite(long pos, byte[] buf, int offset, int length) throws IOException {
        LocalPool.Entry<FileAccess> entry = accessFile();
        try {
            FileAccess file = entry.get();
            file.seek(pos);
            file.write(buf, offset, length);
        } finally {
            entry.release();
        }
    }

    @Override
    protected final void doWrite(long pos, long addr, int length) throws IOException {
        ByteBuffer bb = MemorySegment.ofAddress(addr).reinterpret(length).asByteBuffer();

        boolean interrupted = false;

        LocalPool.Entry<FileAccess> entry = accessFile();
        try {
            FileAccess file = entry.get();
            while (true) try {
                FileChannel channel = file.getChannel();
                while (bb.hasRemaining()) {
                    pos += channel.write(bb, pos);
                }
                break;
            } catch (NonWritableChannelException e) {
                throw new WriteFailureException("File is read only");
            } catch (ClosedByInterruptException e) {
                interrupted = true;
                file = replaceClosedRaf(entry);
            }
        } finally {
            entry.release();
        }

        if (interrupted) {
            // Restore the interrupt status.
            Thread.currentThread().interrupt();
        }
    }

    @Override
    protected Mapping openMapping(boolean readOnly, long pos, int size) throws IOException {
        return new NioMapping(mFile, readOnly, pos, size);
    }

    @Override
    protected final void reopen() throws IOException {
        LocalPool<FileAccess> pool = mFilePool;
        if (pool != null) {
            clearPool(pool);
            testPool();
        }
    }

    @Override
    protected final void doSync(boolean metadata) throws IOException {
        boolean interrupted = false;

        LocalPool.Entry<FileAccess> entry = accessFile();
        try {
            FileAccess file = entry.get();
            while (true) try {
                file.getChannel().force(metadata);
                break;
            } catch (ClosedByInterruptException e) {
                interrupted = true;
                file = replaceClosedRaf(entry);
            }
        } finally {
            entry.release();
        }

        if (interrupted) {
            // Restore the interrupt status.
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public final void close(Throwable cause) throws IOException {
        IOException ex = null;

        mAccessLock.acquireExclusive();
        try {
            if (cause != null && mCause == null) {
                mCause = cause;
            }

            LocalPool<FileAccess> pool = mFilePool;
            if (pool != null) {
                mFilePool = null;
                try {
                    clearPool(pool);
                } catch (IOException e) {
                    ex = e;
                }
            }
        } finally {
            mAccessLock.releaseExclusive();
        }

        try {
            unmap(false);
        } catch (IOException e) {
            if (ex == null) {
                ex = e;
            }
        }

        if (ex != null) {
            throw ex;
        }
    }

    @Override
    public final boolean isClosed() {
        mAccessLock.acquireShared();
        boolean closed = mFilePool == null;
        mAccessLock.releaseShared();
        return closed;
    }

    private LocalPool.Entry<FileAccess> accessFile() throws IOException {
        LocalPool<FileAccess> pool = mFilePool;
        if (pool == null) {
            throw new ClosedChannelException();
        }
        return pool.access();
    }

    private void testPool() throws IOException {
        accessFile().release();
    }

    private FileAccess replaceClosedRaf(LocalPool.Entry<FileAccess> entry) throws IOException {
        // Clear the status to allow retry to succeed.
        Thread.interrupted();
        FileAccess file = openRaf();
        entry.replace(file);
        return file;
    }

    private static void clearPool(LocalPool<FileAccess> pool) throws IOException {
        var consumer = new Consumer<FileAccess>() {
            IOException ex;
                
            @Override
            public void accept(FileAccess file) {
                try {
                    file.close();
                } catch (IOException e) {
                    if (ex == null) {
                        ex = e;
                    }
                }
            }
        };

        pool.clear(consumer);

        if (consumer.ex != null) {
            throw consumer.ex;
        }
    }

    private FileAccess openRaf() throws IOException {
        try {
            return new FileAccess(mFile, mMode);
        } catch (FileNotFoundException e) {
            String message = null;

            if (mFile.isDirectory()) {
                message = "File is a directory";
            } else if (!mFile.isFile()) {
                message = "Not a normal file";
            } else if ("r".equals(mMode)) {
                if (!mFile.exists()) {
                    message = "File does not exist";
                } else if (!mFile.canRead()) {
                    message = "File cannot be read";
                }
            } else {
                if (!mFile.canRead()) {
                    if (!mFile.canWrite()) {
                        message = "File cannot be read or written";
                    } else {
                        message = "File cannot be read";
                    }
                } else if (!mFile.canWrite()) {
                    message = "File cannot be written";
                }
            }

            if (message == null) {
                throw e;
            }

            String path = mFile.getPath();

            String originalMessage = e.getMessage();
            if (!originalMessage.contains(path)) {
                message = message + ": " + mFile.getPath() + ' ' + originalMessage;
            } else {
                message = message + ": " + originalMessage;
            }

            throw new FileNotFoundException(message);
        }
    }

    private static final class FileAccess extends RandomAccessFile {
        private long mPosition;

        FileAccess(File file, String mode) throws IOException {
            super(file, mode);
            seek(0);
        }

        FileChannel positionChannel(long pos) throws IOException {
            mPosition = -1; // seek method must actually seek
            FileChannel channel = getChannel();
            channel.position(pos);
            return channel;
        }

        @Override
        public void seek(long pos) throws IOException {
            if (pos != mPosition) {
                try {
                    super.seek(pos);
                    mPosition = pos;
                } catch (Throwable e) {
                    // Undefined position.
                    mPosition = -1;
                    throw e;
                }
            }
        }

        @Override
        public int read(byte[] buf) throws IOException {
            return read(buf, 0, buf.length);
        }

        @Override
        public int read(byte[] buf, int offset, int length) throws IOException {
            try {
                int amt = super.read(buf, offset, length);
                if (amt > 0) {
                    mPosition += amt;
                }
                return amt;
            } catch (Throwable e) {
                // Undefined position.
                mPosition = -1;
                throw e;
            }
        }

        @Override
        public void write(byte[] buf) throws IOException {
            write(buf, 0, buf.length);
        }

        @Override
        public void write(byte[] buf, int offset, int length) throws IOException {
            try {
                super.write(buf, offset, length);
                mPosition += length;
            } catch (Throwable e) {
                // Undefined position.
                mPosition = -1;
                throw e;
            }
        }

        @Override
        public void setLength(long length) throws IOException {
            // Undefined position. The Windows implementation of the setLength method first
            // sets the position to the desired length, but setting the length might fail. The
            // position isn't repaired, leaving an unexpected side-effect. This is a horrible
            // bug, which hasn't been noticed/fixed in over 20 years. Even in the happy case
            // the position gets modified, so don't trust it all.
            mPosition = -1;

            super.setLength(length);
        }
    }
}
