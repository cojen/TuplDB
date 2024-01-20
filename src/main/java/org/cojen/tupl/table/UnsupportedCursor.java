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

package org.cojen.tupl.table;

import java.io.InputStream;
import java.io.OutputStream;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Ordering;
import org.cojen.tupl.Transaction;

/**
 * Provides non-functional implementations for almost all of the Cursor methods.
 *
 * @author Brian S O'Neill
 */
interface UnsupportedCursor extends Cursor {
    @Override
    default long valueLength() {
        throw new UnsupportedOperationException();
    }

    @Override
    default void valueLength(long length) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int valueRead(long pos, byte[] buf, int off, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void valueWrite(long pos, byte[] buf, int off, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void valueClear(long pos, long length) {
        throw new UnsupportedOperationException();
    }

    @Override
    default InputStream newValueInputStream(long pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    default InputStream newValueInputStream(long pos, int bufferSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    default OutputStream newValueOutputStream(long pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    default OutputStream newValueOutputStream(long pos, int bufferSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    default Ordering ordering() {
        return Ordering.UNSPECIFIED;
    }

    @Override
    default Transaction link(Transaction txn) {
        throw new UnsupportedOperationException();
    }

    @Override
    default Transaction link() {
        throw new UnsupportedOperationException();
    }

    @Override
    default boolean autoload(boolean mode) {
        return true;
    }

    @Override
    default boolean autoload() {
        return true;
    }

    @Override
    default LockResult first() {
        throw new UnsupportedOperationException();
    }

    @Override
    default LockResult last() {
        throw new UnsupportedOperationException();
    }

    @Override
    default LockResult skip(long amount) {
        throw new UnsupportedOperationException();
    }

    @Override
    default LockResult next() {
        throw new UnsupportedOperationException();
    }

    @Override
    default LockResult previous() {
        throw new UnsupportedOperationException();
    }

    @Override
    default LockResult find(byte[] key) {
        throw new UnsupportedOperationException();
    }

    @Override
    default LockResult random(byte[] lowKey, boolean lowInclusive,
                             byte[] highKey, boolean highInclusive)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    default boolean exists() {
        throw new UnsupportedOperationException();
    }

    @Override
    default LockResult load() {
        throw new UnsupportedOperationException();
    }

    @Override
    default void store(byte[] value) {
        throw new UnsupportedOperationException();
    }

    @Override
    default Cursor copy() {
        throw new UnsupportedOperationException();
    }
}
