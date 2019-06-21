/*
 *  Copyright (C) 2019 Cojen.org
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

package org.cojen.tupl.util;

/**
 * Function which is registered with the {@link LatchCondition} {@link
 * LatchCondition#uponSignal uponSignal} method, or the {@link Latch} {@link
 * Latch#uponExclusive uponExclusive} method.
 *
 * @author Brian S O'Neill
 */
@FunctionalInterface
public interface Continuation {
    /**
     * Is called with the exclusive latch held. The implementation may continue in another
     * thread with the latch still held, in which case it must return false. Otherwise, it
     * should return true and let the caller release or transfer the exclusive latch.
     * Explicitly releasing the latch in the calling thread isn't recommended, because it can
     * lead to a stack overflow error.
     *
     * @return true if the latch is held; false if already released or if another thread will
     * release the latch later
     */
    public boolean run();
}
