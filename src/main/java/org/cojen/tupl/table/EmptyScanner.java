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

import org.cojen.tupl.Scanner;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public final class EmptyScanner<R> implements Scanner<R> {
    public static final EmptyScanner THE = new EmptyScanner<Object>();

    @SuppressWarnings("unchecked")
    public static <R> EmptyScanner<R> the() {
        return THE;
    }

    private EmptyScanner() {
    }

    @Override
    public R row() {
        return null;
    }

    @Override
    public R step() {
        return null;
    }

    @Override
    public R step(R dst) {
        return null;
    }

    @Override
    public void close() {
    }

    @Override
    public long estimateSize() {
        return 0;
    }

    @Override
    public int characteristics() {
        return NONNULL | ORDERED | IMMUTABLE | SIZED | SORTED | DISTINCT;
    }
}
