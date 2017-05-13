/*
 *  Copyright (C) 2017 Cojen.org
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

/**
 * 
 *
 * @author Brian S O'Neill
 */
abstract class SelectCombiner implements Combiner {
    static final class First extends SelectCombiner {
        static final Combiner THE = new First();

        @Override
        public byte[] combine(byte[] key, byte[] first, byte[] second) {
            return first;
        }
    };

    static final class Second extends SelectCombiner {
        static final Combiner THE = new Second();

        @Override
        public byte[] combine(byte[] key, byte[] first, byte[] second) {
            return second;
        }
    };

    static final class Discard extends SelectCombiner {
        static final Combiner THE = new Discard();

        @Override
        public byte[] combine(byte[] key, byte[] first, byte[] second) {
            return null;
        }
    };

    @Override
    public boolean requireValues() {
        return false;
    }
}
