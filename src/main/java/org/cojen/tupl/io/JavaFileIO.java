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
import java.io.InterruptedIOException;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.nio.ByteBuffer;

import java.nio.channels.FileChannel;
import java.nio.channels.ClosedByInterruptException;

import java.util.EnumSet;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.LatchCondition;

import static org.cojen.tupl.io.Utils.*;

/**
 * Basic FileIO implementation which uses the Java RandomAccessFile class,
 * unless a more suitable implementation is available.
 *
 * @author Brian S O'Neill
 */
final class JavaFileIO extends AbstractFileIO {
    private final File mFile;
    private final String mMode;

    // Access these file pool fields using this latch.
    private final Latch mFilePoolLatch;
    private final LatchCondition mFilePoolCondition;
    private final FileAccess[] mFilePool;
    private int mFilePoolTop;

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

        if (openFileCount < 1) {
            openFileCount = 1;
        }

        mFilePoolLatch = new Latch();
        mFilePoolCondition = new LatchCondition();
        mFilePool = new FileAccess[openFileCount];

        try {
            mFilePoolLatch.acquireExclusive();
            try {
                for (int i=0; i<openFileCount; i++) {
                    mFilePool[i] = openRaf();
                }
            } finally {
                mFilePoolLatch.releaseExclusive();
            }
        } catch (Throwable e) {
            throw closeOnFailure(this, e);
        }

        if (allowMap && options.contains(OpenOption.MAPPED)) {
            map();
        }

        if (options.contains(OpenOption.CREATE)) {
            dirSync(file);
        }
    }

    @Override
    public boolean isDirectIO() {
        return false;
    }

    @Override
    protected long doLength() throws IOException {
        FileAccess file = accessFile();
        try {
            return file.length();
        } finally {
            yieldFile(file);
        }
    }

    @Override
    protected void doSetLength(long length) throws IOException {
        FileAccess file = accessFile();
        try {
            file.setLength(length);
        } finally {
            yieldFile(file);
        }
    }

    @Override
    protected void doRead(long pos, byte[] buf, int offset, int length) throws IOException {
        try {
            FileAccess file = accessFile();
            try {
                file.seek(pos);
                file.readFully(buf, offset, length);
            } finally {
                yieldFile(file);
            }
        } catch (EOFException e) {
            throw new EOFException("Attempt to read past end of file: " + pos);
        }
    }

    @Override
    protected void doRead(long pos, byte[] buf, int offset, int length, ByteBuffer tail)
        throws IOException
    {
        doRead(pos, ByteBuffer.wrap(buf, offset, length), tail);
    }

    @Override
    protected void doRead(long pos, ByteBuffer bb) throws IOException {
        boolean interrupted = false;

        FileAccess file = accessFile();
        try {
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
                // Clear the status to allow retry to succeed.
                Thread.interrupted();
                file = openRaf();
            }
        } finally {
            yieldFile(file);
        }

        if (interrupted) {
            // Restore the interrupt status.
            Thread.currentThread().interrupt();
        }
    }

    @Override
    protected void doRead(long pos, ByteBuffer bb, ByteBuffer tail) throws IOException {
        boolean interrupted = false;

        FileAccess file = accessFile();
        try {
            while (true) try {
                FileChannel channel = file.positionChannel(pos);
                while (bb.hasRemaining()) {
                    long amt = channel.read(new ByteBuffer[] {bb, tail});
                    if (amt < 0) {
                        throw new EOFException
                            ("Attempt to read past end of file: " + channel.position());
                    }
                    pos += amt;
                }
                while (tail.hasRemaining()) {
                    int amt = channel.read(tail);
                    if (amt < 0) {
                        throw new EOFException
                            ("Attempt to read past end of file: " + channel.position());
                    }
                    pos += amt;
                }
                break;
            } catch (ClosedByInterruptException e) {
                interrupted = true;
                // Clear the status to allow retry to succeed.
                Thread.interrupted();
                file = openRaf();
            }
        } finally {
            yieldFile(file);
        }

        if (interrupted) {
            // Restore the interrupt status.
            Thread.currentThread().interrupt();
        }
    }

    @Override
    protected void doWrite(long pos, byte[] buf, int offset, int length) throws IOException {
        FileAccess file = accessFile();
        try {
            file.seek(pos);
            file.write(buf, offset, length);
        } finally {
            yieldFile(file);
        }
    }

    @Override
    protected void doWrite(long pos, byte[] buf, int offset, int length, ByteBuffer tail)
        throws IOException
    {
        doWrite(pos, ByteBuffer.wrap(buf, offset, length), tail);
    }

    @Override
    protected void doWrite(long pos, ByteBuffer bb) throws IOException {
        boolean interrupted = false;

        FileAccess file = accessFile();
        try {
            while (true) try {
                FileChannel channel = file.getChannel();
                while (bb.hasRemaining()) {
                    pos += channel.write(bb, pos);
                }
                break;
            } catch (ClosedByInterruptException e) {
                interrupted = true;
                // Clear the status to allow retry to succeed.
                Thread.interrupted();
                file = openRaf();
            }
        } finally {
            yieldFile(file);
        }

        if (interrupted) {
            // Restore the interrupt status.
            Thread.currentThread().interrupt();
        }
    }

    @Override
    protected void doWrite(long pos, ByteBuffer bb, ByteBuffer tail) throws IOException {
        boolean interrupted = false;

        FileAccess file = accessFile();
        try {
            while (true) try {
                FileChannel channel = file.positionChannel(pos);
                while (bb.hasRemaining()) {
                    pos += channel.write(new ByteBuffer[] {bb, tail});
                }
                while (tail.hasRemaining()) {
                    pos += channel.write(tail);
                }
                break;
            } catch (ClosedByInterruptException e) {
                interrupted = true;
                // Clear the status to allow retry to succeed.
                Thread.interrupted();
                file = openRaf();
            }
        } finally {
            yieldFile(file);
        }

        if (interrupted) {
            // Restore the interrupt status.
            Thread.currentThread().interrupt();
        }
    }

    @Override
    protected Mapping openMapping(boolean readOnly, long pos, int size) throws IOException {
        return Mapping.open(mFile, readOnly, pos, size);
    }

    @Override
    protected void reopen() throws IOException {
        // Caller should hold mAccessLock exclusively.

        IOException ex = null;

        mFilePoolLatch.acquireExclusive();
        try {
            try {
                closePool();
            } catch (IOException e) {
                ex = e;
            }

            for (int i=0; i<mFilePool.length; i++) {
                try {
                    mFilePool[i] = openRaf();
                } catch (IOException e) {
                    if (ex == null) {
                        ex = e;
                    }
                }
            }
        } finally {
            mFilePoolLatch.releaseExclusive();
        }

        if (ex != null) {
            throw ex;
        }
    }

    @Override
    protected void doSync(boolean metadata) throws IOException {
        boolean interrupted = false;

        FileAccess file = accessFile();
        try {
            while (true) try {
                file.getChannel().force(metadata);
                break;
            } catch (ClosedByInterruptException e) {
                interrupted = true;
                // Clear the status to allow retry to succeed.
                Thread.interrupted();
                file = openRaf();
            }
        } finally {
            yieldFile(file);
        }

        if (interrupted) {
            // Restore the interrupt status.
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close(Throwable cause) throws IOException {
        IOException ex = null;

        mAccessLock.acquireExclusive();
        try {
            if (cause != null && mCause == null) {
                mCause = cause;
            }

            mFilePoolLatch.acquireExclusive();
            try {
                closePool();
            } catch (IOException e) {
                ex = e;
            } finally {
                mFilePoolLatch.releaseExclusive();
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

    // Caller must hold mAccessLock exclusively and also synchronize on mFilePool.
    private void closePool() throws IOException {
        if (mFilePoolTop != 0) {
            throw new AssertionError();
        }

        IOException ex = null;

        for (FileAccess file : mFilePool) {
            if (file != null) {
                try {
                    file.close();
                } catch (IOException e) {
                    if (ex == null) {
                        ex = e;
                    }
                }
            }
        }

        if (ex != null) {
            throw ex;
        }
    }

    private FileAccess accessFile() throws InterruptedIOException {
        FileAccess[] pool = mFilePool;
        Latch latch = mFilePoolLatch;
        latch.acquireExclusive();
        try {
            int top;
            while ((top = mFilePoolTop) == pool.length) {
                if (mFilePoolCondition.await(latch) < 1) {
                    throw new InterruptedIOException();
                }
            }
            FileAccess file = pool[top];
            mFilePoolTop = top + 1;
            return file;
        } finally {
            latch.releaseExclusive();
        }
    }

    private void yieldFile(FileAccess file) {
        FileAccess[] pool = mFilePool;
        Latch latch = mFilePoolLatch;
        latch.acquireExclusive();
        try {
            pool[--mFilePoolTop] = file;
            mFilePoolCondition.signal(latch);
        } finally {
            latch.releaseExclusive();
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

    static class FileAccess extends RandomAccessFile {
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
