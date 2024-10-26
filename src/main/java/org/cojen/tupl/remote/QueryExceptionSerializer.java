/*
 *  Copyright (C) 2024 Cojen.org
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

import org.cojen.tupl.QueryException;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class QueryExceptionSerializer implements Serializer {
    static final QueryExceptionSerializer THE = new QueryExceptionSerializer();

    private QueryExceptionSerializer() {
    }

    @Override
    public Set<Class<?>> supportedTypes() {
        return Set.of(QueryException.class);
    }

    @Override
    public void write(Pipe pipe, Object obj) throws IOException {
        var e = (QueryException) obj;
        pipe.writeObject(e.getStackTrace());
        pipe.writeObject(e.baseMessage());
        pipe.writeInt(e.startPos());
        pipe.writeInt(e.endPos());
    }

    @Override
    public Object read(Pipe pipe) throws IOException {
        var trace = (StackTraceElement[]) pipe.readObject();
        var message = (String) pipe.readObject();
        int startPos = pipe.readInt();
        int endPos = pipe.readInt();
        var e = new QueryException(message, startPos, endPos);
        e.setStackTrace(trace);
        return e;
    }
}
