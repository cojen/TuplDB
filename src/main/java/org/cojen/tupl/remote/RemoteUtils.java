/*
 *  Copyright (C) 2022 Cojen.org
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

import java.util.concurrent.TimeUnit;

import org.cojen.dirmi.Environment;
import org.cojen.dirmi.Serializer;

import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Ordering;

import org.cojen.tupl.diag.DatabaseStats;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RemoteUtils {
    public static Environment createEnvironment() {
        Environment env = Environment.create();

        env.customSerializers
            (Serializer.simple(DatabaseStats.class),
             Serializer.simple(TimeUnit.class),
             Serializer.simple(DurabilityMode.class),
             Serializer.simple(LockMode.class),
             Serializer.simple(LockResult.class),
             Serializer.simple(Ordering.class),
             LockTimeoutExceptionSerializer.THE,
             DeadlockInfoSerializer.THE,
             DeadlockExceptionSerializer.THE);

        return env;
    }
}
