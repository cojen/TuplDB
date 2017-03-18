/*
 *  Copyright 2017 Cojen.org
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
 * 
 *
 * @author Brian S O'Neill
 */
final class KeyOnlyCursor extends WrappedCursor<Cursor> {
    KeyOnlyCursor(Cursor source) {
        super(source);
        source.autoload(false);
    }

    @Override
    public byte[] value() {
        return KeyOnlyView.valueScrub(source.value());
    }

    @Override
    public boolean autoload(boolean mode) {
        return false;
    }

    @Override
    public boolean autoload() {
        return false;
    }

    @Override
    public LockResult load() throws IOException {
        return source.lock();
    }

    @Override
    public void store(byte[] value) throws IOException {
        KeyOnlyView.valueCheck(value);
        source.store(null);
    }

    @Override
    public void commit(byte[] value) throws IOException {
        KeyOnlyView.valueCheck(value);
        source.commit(null);
    }

    @Override
    public Cursor copy() {
        return new KeyOnlyCursor(source.copy());
    }
}
