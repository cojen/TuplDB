/*
 *  Copyright (C) 2025 Cojen.org
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

import java.io.IOException;

import java.util.Comparator;

import org.cojen.tupl.Updater;

/**
 * @author Brian S. O'Neill
 * @see MergeQuery
 */
final class MergeUpdater<R> extends MergeScanner<R> implements Updater<R> {
    MergeUpdater(Updater<R>[] sources, Comparator<R> c) throws IOException {
        super(sources, c);
    }

    @Override
    public R update(R dst) throws IOException {
        Updater<R> current = current();
        if (current == null) {
            return null;
        }
        R row = current.update(dst);
        if (row != null) {
            dst = null; // cannot share dst among the sources
        }
        return step(dst);
    }

    @Override
    public R delete(R dst) throws IOException {
        Updater<R> current = current();
        if (current == null) {
            return null;
        }
        R row = current.delete(dst);
        if (row != null) {
            dst = null; // cannot share dst among the sources
        }
        return step(dst);
    }

    @Override
    protected Updater<R> current() {
        return (Updater<R>) super.current();
    }
}
