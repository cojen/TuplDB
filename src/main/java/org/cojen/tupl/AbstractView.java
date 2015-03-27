/*
 *  Copyright 2015 Brian S O'Neill
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
 * Implements a few {@link View} methods.
 *
 * @author Brian S O'Neill
 */
public abstract class AbstractView implements View {
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean insert(Transaction txn, byte[] key, byte[] value) throws IOException {
        return update(txn, key, null, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean delete(Transaction txn, byte[] key) throws IOException {
        return replace(txn, key, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(Transaction txn, byte[] key, byte[] value) throws IOException {
        return update(txn, key, value, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public View viewGe(byte[] key) {
        return BoundedView.viewGe(this, key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public View viewGt(byte[] key) {
        return BoundedView.viewGt(this, key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public View viewLe(byte[] key) {
        return BoundedView.viewLe(this, key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public View viewLt(byte[] key) {
        return BoundedView.viewLt(this, key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public View viewPrefix(byte[] prefix, int trim) {
        return BoundedView.viewPrefix(this, prefix, trim);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public View viewTransformed(Transformer transformer) {
        return TransformedView.apply(this, transformer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public View viewReverse() {
        return new ReverseView(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public View viewUnmodifiable() {
        return UnmodifiableView.apply(this);
    }
}
