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

package org.cojen.tupl;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.util.Comparator;

/**
 * Abstract wrapper around another cursor. Subclass must implement the {@link #copy copy}
 * method, and it should also override the {@link #store store} and {@link #commit commit}
 * methods.
 *
 * @author Brian S O'Neill
 */
public abstract class WrappedCursor<C extends Cursor> implements Cursor {
    protected final C source;

    protected WrappedCursor(C source) {
        this.source = source;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long valueLength() throws IOException {
        return source.valueLength();
    }

    /**
     * Always throws UnmodifiableViewException by default.
     */
    @Override
    public void setValueLength(long length) throws IOException {
        throw new UnmodifiableViewException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int valueRead(long pos, byte[] buf, int off, int len) throws IOException {
        return source.valueRead(pos, buf, off, len);
    }

    /**
     * Always throws UnmodifiableViewException by default.
     */
    @Override
    public void valueWrite(long pos, byte[] buf, int off, int len) throws IOException {
        throw new UnmodifiableViewException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream newValueInputStream(long pos) throws IOException {
        return source.newValueInputStream(pos);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream newValueInputStream(long pos, int bufferSize) throws IOException {
        return source.newValueInputStream(pos, bufferSize);
    }

    /**
     * Always throws UnmodifiableViewException by default.
     */
    @Override
    public OutputStream newValueOutputStream(long pos) throws IOException {
        throw new UnmodifiableViewException();
    }

    /**
     * Always throws UnmodifiableViewException by default.
     */
    @Override
    public OutputStream newValueOutputStream(long pos, int bufferSize) throws IOException {
        throw new UnmodifiableViewException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Ordering getOrdering() {
        return source.getOrdering();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Comparator<byte[]> getComparator() {
        return source.getComparator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Transaction link(Transaction txn) {
        return source.link(txn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Transaction link() {
        return source.link();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] key() {
        return source.key();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] value() {
        return source.value();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean autoload(boolean mode) {
        return source.autoload(mode);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean autoload() {
        return source.autoload();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareKeyTo(byte[] rkey) {
        return source.compareKeyTo(rkey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareKeyTo(byte[] rkey, int offset, int length) {
        return source.compareKeyTo(rkey, offset, length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult first() throws IOException {
        return source.first();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult last() throws IOException {
        return source.last();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult skip(long amount) throws IOException {
        return source.skip(amount);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult skip(long amount, byte[] limitKey, boolean inclusive) throws IOException {
        return source.skip(amount, limitKey, inclusive);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult next() throws IOException {
        return source.next();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult nextLe(byte[] limitKey) throws IOException {
        return source.nextLe(limitKey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult nextLt(byte[] limitKey) throws IOException {
        return source.nextLt(limitKey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult previous() throws IOException {
        return source.previous();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult previousGe(byte[] limitKey) throws IOException {
        return source.previousGe(limitKey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult previousGt(byte[] limitKey) throws IOException {
        return source.previousGt(limitKey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult find(byte[] key) throws IOException {
        return source.find(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult findGe(byte[] key) throws IOException {
        return source.findGe(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult findGt(byte[] key) throws IOException {
        return source.findGt(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult findLe(byte[] key) throws IOException {
        return source.findLe(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult findLt(byte[] key) throws IOException {
        return source.findLt(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult findNearby(byte[] key) throws IOException {
        return source.findNearby(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult findNearbyGe(byte[] key) throws IOException {
        return source.findNearbyGe(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult findNearbyGt(byte[] key) throws IOException {
        return source.findNearbyGt(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult findNearbyLe(byte[] key) throws IOException {
        return source.findNearbyLe(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult findNearbyLt(byte[] key) throws IOException {
        return source.findNearbyLt(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult random(byte[] lowKey, byte[] highKey) throws IOException {
        return source.random(lowKey, highKey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult lock() throws IOException {
        return source.lock();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult load() throws IOException {
        return source.load();
    }

    /**
     * Always throws UnmodifiableViewException by default.
     */
    @Override
    public void store(byte[] value) throws IOException {
        throw new UnmodifiableViewException();
    }

    /**
     * Always throws UnmodifiableViewException by default.
     */
    @Override
    public void commit(byte[] value) throws IOException {
        throw new UnmodifiableViewException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reset() {
        source.reset();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        source.close();
    }
}
