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

import java.io.IOException;

import java.util.Set;

import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.Serializer;

import org.cojen.tupl.DeadlockException;

import org.cojen.tupl.diag.DeadlockInfo;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public final class DeadlockExceptionSerializer implements Serializer {
    static final DeadlockExceptionSerializer THE = new DeadlockExceptionSerializer();

    private DeadlockExceptionSerializer() {
    }

    @Override
    public Set<Class<?>> supportedTypes() {
        return Set.of(DeadlockException.class);
    }

    @Override
    public void write(Pipe pipe, Object obj) throws IOException {
        var e = (DeadlockException) obj;
        pipe.writeObject(e.getStackTrace());
        pipe.writeLong(e.timeoutNanos());
        Object att = e.ownerAttachment();
        if (att != null) {
            pipe.writeObject(att.toString());
        } else {
            pipe.writeNull();
        }
        pipe.writeBoolean(e.isGuilty());
        pipe.writeObject(e.deadlockSet());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object read(Pipe pipe) throws IOException {
        var trace = (StackTraceElement[]) pipe.readObject();
        long timeoutNanos = pipe.readLong();
        Object att = pipe.readObject();
        boolean guilty = pipe.readBoolean();
        var set = (Set<DeadlockInfo>) pipe.readObject();
        var e = new DeadlockException(timeoutNanos, att, guilty, set);
        e.setStackTrace(trace);
        return e;
    }
}
