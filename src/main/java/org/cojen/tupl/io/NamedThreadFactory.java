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

package org.cojen.tupl.io;

import java.util.concurrent.ThreadFactory;

/**
 * Creates daemon threads with a specified name prefix.
 *
 * @author Brian S O'Neill
 */
final class NamedThreadFactory implements ThreadFactory {
    private static int cThreadCounter;

    private final String mPrefix;
    private final ThreadGroup mGroup;

    NamedThreadFactory(String prefix) {
        mPrefix = prefix == null ? "Thread" : prefix;
        SecurityManager sm = System.getSecurityManager();
        mGroup = (sm != null) ? sm.getThreadGroup() : Thread.currentThread().getThreadGroup();
    }

    @Override
    public Thread newThread(Runnable r) {
        int num;
        synchronized (NamedThreadFactory.class) {
            num = ++cThreadCounter;
        }
        Thread t = new Thread(mGroup, r, mPrefix + '-' + (num & 0xffffffffL));
        t.setDaemon(true);
        return t;
    }
}
