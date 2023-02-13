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

import org.cojen.dirmi.RemoteException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ClientDeleteIndex implements Runnable {
    private ClientIndex mIndex;
    private RemoteDeleteIndex mRemote;

    ClientDeleteIndex(ClientIndex ix, RemoteDeleteIndex remote) {
        mIndex = ix;
        mRemote = remote;
    }

    @Override
    public void run() {
        ClientIndex index;
        RemoteDeleteIndex remote;

        synchronized (this) {
            index = mIndex;
            remote = mRemote;
            if (index == null || remote == null) {
                return;
            }
            mIndex = null;
            mRemote = null;
        }

        index.close(false, false);

        try {
            remote.run();
        } catch (RemoteException e) {
            // Ignore.
        }
    }
}
