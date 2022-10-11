/*
 *  Copyright (C) 2022 Cojen.org
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

package org.cojen.tupl.remote;

import java.io.IOException;

import java.util.Comparator;

import org.cojen.dirmi.Pipe;

import org.cojen.tupl.Scanner;

import org.cojen.tupl.core.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ClientScanner implements Scanner {
    private Pipe mPipe;
    private byte[] mKey, mValue;

    ClientScanner(Pipe pipe) throws IOException {
        // The initial request is pending until flush is called.
        pipe.flush();
        mPipe = pipe;
        pipe.read(); // TODO: comparator type
        step();
    }

    @Override
    public Comparator<byte[]> comparator() {
        // TODO: comparator
        return null;
    }

    @Override
    public byte[] key() {
        return mKey;
    }

    @Override
    public byte[] value() {
        return mValue;
    }

    @Override
    public boolean step() throws IOException {
        Pipe pipe = mPipe;

        if (pipe != null) {
            Object obj;
            try {
                obj = mPipe.readObject();
                if (obj instanceof byte[]) {
                    mKey = (byte[]) obj;
                    mValue = (byte[]) mPipe.readObject();
                    return true;
                }
                mKey = null;
                mValue = null;
                pipe.recycle();
            } catch (Throwable e) {
                throw Utils.fail(this, e);
            }

            if (obj instanceof Throwable) {
                throw Utils.rethrow((Throwable) obj);
            }
        }

        return false;
    }

    @Override
    public void close() throws IOException {
        mKey = null;
        mValue = null;
        Pipe pipe = mPipe;
        if (pipe != null) {
            mPipe = null;
            pipe.close();
        }
    }
}
