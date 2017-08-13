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

package org.cojen.tupl.repl;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;

import java.net.Socket;
import java.net.SocketAddress;

import java.util.Collections;
import java.util.Map;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class SocketSnapshotSender extends OutputStream implements SnapshotSender {
    private final Socket mSocket;
    private final OutputStream mOut;
    private final Map<String, String> mOptions;

    SocketSnapshotSender(Socket socket) throws IOException {
        OptionsDecoder dec;
        try {
            dec = new OptionsDecoder(socket.getInputStream());
        } catch (EOFException e) {
            Utils.closeQuietly(e, socket);
            throw new IOException("Disconnected");
        }

        int encoding = dec.decodeIntLE();
        if (encoding != 0) {
            Utils.closeQuietly(null, socket);
            throw new IOException("Unknown encoding: " + encoding);
        }

        mSocket = socket;
        mOut = socket.getOutputStream();
        mOptions = dec.decodeMap();
    }

    @Override
    public SocketAddress receiverAddress() {
        return mSocket.getRemoteSocketAddress();
    }

    @Override
    public Map<String, String> options() {
        return mOptions;
    }

    @Override
    public OutputStream begin(long length, long index, Map<String, String> options)
        throws IOException
    {
        OptionsEncoder enc = new OptionsEncoder();
        enc.encodeIntLE(0); // encoding format
        enc.encodeLongLE(length);
        enc.encodeLongLE(index);
        enc.encodeMap(options == null ? Collections.emptyMap() : options);
        enc.writeTo(this);

        return this;
    }

    @Override
    public void write(int b) throws IOException {
        mOut.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        mOut.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        mOut.flush();
    }

    @Override
    public void close() throws IOException {
        // FIXME: unregister from Controller
        mSocket.close();
    }
}
