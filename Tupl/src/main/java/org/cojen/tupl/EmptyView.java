/*
 *  Copyright 2012 Brian S O'Neill
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

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class EmptyView implements View {
    static final EmptyView THE = new EmptyView();

    private EmptyView() {
    }

    @Override
    public Cursor newCursor(Transaction txn) {
        return new EmptyCursor(txn);
    }

    @Override
    public byte[] load(Transaction txn, byte[] key) {
        return null;
    }

    @Override
    public void store(Transaction txn, byte[] key, byte[] value) {
        throw new IllegalArgumentException("Key is outside allowed range");
    }

    @Override
    public boolean insert(Transaction txn, byte[] key, byte[] value) {
        throw new IllegalArgumentException("Key is outside allowed range");
    }

    @Override
    public boolean replace(Transaction txn, byte[] key, byte[] value) {
        return false;
    }

    @Override
    public boolean update(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue) {
        if (oldValue == null) {
            if (newValue == null) {
                return true;
            }
            throw new IllegalArgumentException("Key is outside allowed range");
        }
        return false;
    }

    @Override
    public boolean delete(Transaction txn, byte[] key) {
        return false;
    }

    @Override
    public boolean remove(Transaction txn, byte[] key, byte[] value) {
        return value == null;
    }

    @Override
    public View viewGe(byte[] key) {
        return this;
    }

    @Override
    public View viewGt(byte[] key) {
        return this;
    }

    @Override
    public View viewLe(byte[] key) {
        return this;
    }

    @Override
    public View viewLt(byte[] key) {
        return this;
    }

    @Override
    public View viewReverse() {
        return this;
    }
}
