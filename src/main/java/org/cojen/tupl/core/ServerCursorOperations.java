package org.cojen.tupl.core;

import org.cojen.dirmi.Pipe;
import org.cojen.tupl.Cursor;
import org.cojen.tupl.remote.RemoteOutputControl;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

final public class ServerCursorOperations {

    public static void readTransfer(long pos, int bufferSize, Pipe pipe, Cursor mCursor) throws IOException {
        // Limiting bufferSize to 0x7ffe allows forms 0x7fff and 0xffff to indicate special
        // conditions. Currently only 0xffff is used, and it indicates a terminal exception.

        // Act upon a copy of the cursor to guard against any odd thread-safety issues. Note
        // that the cursor stream isn't explicitly closed. The implementation doesn't hold onto
        // any extra resources, and so it doesn't need to be closed to free memory.
        doTransfer: try (Cursor c = mCursor.copy()) {
            byte[] buf;
            InputStream in;
            try {
                buf = new byte[2 + bufferSize];
                in = c.newValueInputStream(pos, 0);
            } catch (Throwable e) {
                pipe.writeShort(0xffff); // indicates an exception
                pipe.writeObject(e);
                break doTransfer;
            }

            while (true) {
                int amt;
                try {
                    amt = in.readNBytes(buf, 2, bufferSize);
                } catch (Throwable e) {
                    pipe.writeShort(0xffff); // indicates an exception
                    pipe.writeObject(e);
                    break doTransfer;
                }

                if (amt < bufferSize) {
                    amt = Math.max(amt, 0);
                    // Indicate that the end is reached by setting the high bit.
                    Utils.encodeShortBE(buf, 0, amt | 0x8000);
                    pipe.write(buf, 0, 2 + amt);
                    break doTransfer;
                }

                Utils.encodeShortBE(buf, 0, amt);
                pipe.write(buf, 0, 2 + amt);
            }
        }

        pipe.flush();

        // Wait for ack before the pipe can be safely recycled.

        int ack = pipe.read();
        if (ack < 0) {
            pipe.close();
        } else {
            pipe.recycle();
        }
    }

    public static void writeTransfer(long pos, Pipe pipe, Cursor mCursor) throws IOException {
        var control = (RemoteOutputControl) pipe.readObject();

        // Act upon a copy of the cursor to guard against any odd thread-safety issues. Note
        // that the cursor stream isn't explicitly closed. The implementation doesn't hold onto
        // any extra resources, and so it doesn't need to be closed to free memory.
        try (Cursor c = mCursor.copy()) {
            OutputStream out = c.newValueOutputStream(pos, 0);

            while (true) {
                int header = pipe.readUnsignedShort();
                int length = header & 0x7fff;
                if (length != 0) {
                    if (pipe.transferTo(out, length) < length) {
                        throw new EOFException();
                    }
                    if ((header & 0x8000) != 0) {
                        break;
                    }
                } else {
                    if (header == 0x8000) {
                        break;
                    }
                    // A plain empty chunk indicates that a flush ack is requested.
                    pipe.write(1); // write flush ack
                    pipe.flush();
                }
            }

            pipe.write(2); // write close ack
            pipe.flush();
            pipe.recycle();
        } catch (Throwable e) {
            control.exception(e);
            pipe.close();
            return;
        }

        control.dispose();
    }
}