/*
 *  Copyright (C) 2011-2017 Cojen.org
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

package org.cojen.tupl.core;

import java.io.IOException;

import org.cojen.tupl.Crypto;
import org.cojen.tupl.Database;
import org.cojen.tupl.EventListener;

/**
 * Provides access to features of LocalDatabase implementation class, to reduce the number of
 * generated classes. In particular, there's no generated _Checkpointer class because
 * Checkpointer references only CoreDatabase instead of LocalDatabase. Without this, a
 * _Checkpointer would be generated to reference the generated _LocalDatabase.
 *
 * @author Brian S O'Neill
 */
abstract class CoreDatabase implements Database {
    abstract boolean isDirectPageAccess();

    abstract boolean isCacheOnly();

    abstract boolean isReadOnly();

    abstract Crypto dataCrypto();

    abstract Tree registry();

    /**
     * @return null if none
     */
    abstract EventListener eventListener();

    /**
     * Called by Checkpointer task.
     */
    abstract void checkpoint(long sizeThreshold, long delayThresholdNanos) throws IOException;

    /**
     * Called by ReplManager.
     */
    abstract long writeControlMessage(byte[] message) throws IOException;
}
