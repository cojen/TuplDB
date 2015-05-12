/*
 *  Copyright 2013 Brian S O'Neill
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

import java.io.IOException;

/**
 * Abstract wrapper around another cursor. Subclass must implement the {@link #copy copy}
 * method, and it should also override the {@link #store store} and {@link #newStream
 * newStream} methods.
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
    public Ordering getOrdering() {
        return source.getOrdering();
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
    public LockResult random(byte[] lowKey, byte[] highKey) throws IOException {
        return source.random(lowKey, highKey);
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
     * Returns an unmodifiable stream by default.
     */
    @Override
    public Stream newStream() {
        return new UnmodifiableStream(source.newStream());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reset() {
        source.reset();
    }
}
