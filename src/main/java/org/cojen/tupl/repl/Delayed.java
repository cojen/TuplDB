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

package org.cojen.tupl.repl;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
abstract class Delayed implements Comparable<Delayed>, Runnable {
    protected long mCounter;

    Delayed(long counter) {
        mCounter = counter;
    }

    @Override
    public int compareTo(Delayed other) {
        return Long.signum(mCounter - other.mCounter);
    }

    /**
     * Runs with the original counter value, never throwing an exception.
     */
    public final void run() {
        run(mCounter);
    }

    /**
     * Runs with the given updated counter value, never throwing an exception.
     */
    public final void run(long counter) {
        try {
            doRun(counter);
        } catch (Throwable e) {
            Utils.uncaught(e);
        }
    }

    /**
     * @param counter current counter value
     */
    protected abstract void doRun(long counter) throws Throwable;

    static final class Runner extends Delayed {
        private final Runnable mTask;

        Runner(long counter, Runnable task) {
            super(counter);
            mTask = task;
        }

        @Override
        protected void doRun(long counter) throws Throwable {
            mTask.run();
        }
    }
}
