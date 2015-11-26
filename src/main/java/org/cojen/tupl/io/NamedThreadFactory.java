/*
 *  Copyright 2013-2015 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
