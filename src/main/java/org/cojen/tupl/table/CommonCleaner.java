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

import java.lang.ref.Cleaner;
import java.lang.ref.SoftReference;

/**
 * Provides access to a shared Cleaner instance, backed by one thread.
 *
 * @author Brian S O'Neill
 */
public final class CommonCleaner {
    private static volatile SoftReference<Cleaner> cCleanerRef;

    public static Cleaner access() {
        SoftReference<Cleaner> cleanerRef = cCleanerRef;
        Cleaner cleaner;
        if (cleanerRef == null || (cleaner = cleanerRef.get()) == null) {
            synchronized (CommonCleaner.class) {
                cleanerRef = cCleanerRef;
                if (cleanerRef == null || (cleaner = cleanerRef.get()) == null) {
                    cleaner = Cleaner.create();
                    cCleanerRef = new SoftReference<>(cleaner);
                }
            }
        }

        return cleaner;
    }
}
