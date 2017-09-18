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
import java.io.InputStream;
import java.io.IOException;

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
final class SocketSnapshotReceiver implements SnapshotReceiver {
    private final Socket mSocket;
    private final long mLength;
    private final long mPrevTerm;
    private final long mTerm;
    private final long mIndex;
    private final Map<String, String> mOptions;

    SocketSnapshotReceiver(GroupFile groupFile, Socket socket, Map<String, String> requestOptions)
        throws IOException
    {
        OptionsEncoder enc = new OptionsEncoder();
        enc.encodeIntLE(0); // encoding format
        enc.encodeMap(requestOptions == null ? Collections.emptyMap() : requestOptions);
        enc.writeTo(socket.getOutputStream());

        OptionsDecoder dec;
        try {
            dec = new OptionsDecoder(socket.getInputStream());
        } catch (EOFException e) {
            Utils.closeQuietly(socket);
            throw new IOException("Disconnected");
        }

        int encoding = dec.decodeIntLE();
        if (encoding != 0) {
            Utils.closeQuietly(socket);
            throw new IOException("Unknown encoding: " + encoding);
        }

        mSocket = socket;
        mLength = dec.decodeLongLE(); 
        mPrevTerm = dec.decodeLongLE(); 
        mTerm = dec.decodeLongLE();
        mIndex = dec.decodeLongLE();
        mOptions = dec.decodeMap();

        groupFile.readFrom(socket.getInputStream());
    }

    @Override
    public SocketAddress senderAddress() {
        return mSocket.getRemoteSocketAddress();
    }

    @Override
    public Map<String, String> options() {
        return mOptions;
    }

    @Override
    public long length() {
        return mLength;
    }

    public long prevTerm() {
        return mPrevTerm;
    }

    public long term() {
        return mTerm;
    }

    @Override
    public long index() {
        return mIndex;
    }

    @Override
    public InputStream inputStream() throws IOException {
        return mSocket.getInputStream();
    }

    @Override
    public void close() throws IOException {
        mSocket.close();
    }

    @Override
    public String toString() {
        return "SnapshotReceiver: {sender=" + senderAddress() + ", length=" + length() +
            ", prevTerm=" + prevTerm() + ", term=" + term() + ", index=" + index() + '}';
    }
}
