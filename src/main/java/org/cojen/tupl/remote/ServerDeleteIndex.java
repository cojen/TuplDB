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

import org.cojen.dirmi.Session;
import org.cojen.dirmi.SessionAware;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ServerDeleteIndex implements RemoteDeleteIndex, SessionAware {
    private final Runnable mTask;

    ServerDeleteIndex(Runnable task) {
        mTask = task;
    }

    @Override
    public void attached(Session<?> session) {
    }

    @Override
    public void detached(Session<?> session) {
        dispose();
    }

    @Override
    public void run() {
        dispose();
    }

    @Override
    public void dispose() {
        mTask.run();
    }
}
