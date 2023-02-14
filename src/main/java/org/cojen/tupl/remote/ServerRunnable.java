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

package org.cojen.tupl.remote;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ServerRunnable implements RemoteRunnable {
    private Runnable mTask;
    private Runnable mFinishTask;

    ServerRunnable(Runnable task) {
        mTask = task;
    }

    @Override
    public void run() {
        // RemoteRunnable is one-shot, and calling run disposes it. Calling this method again
        // has no effect.

        Runnable task, finishTask;

        synchronized (this) {
            task = mTask;
            if (task == null) {
                return;
            }
            mTask = null;
            finishTask = mFinishTask;
            mFinishTask = null;
        }

        try {
            task.run();
        } finally {
            if (finishTask != null) {
                finishTask.run();
            }
        }
    }

    void finishTask(Runnable task) {
        synchronized (this) {
            if (mTask != null) {
                mFinishTask = task;
                return;
            }
        }

        task.run();
    }
}
