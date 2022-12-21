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

import java.io.DataInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.util.concurrent.TimeUnit;

import org.cojen.dirmi.Environment;
import org.cojen.dirmi.Serializer;

import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Ordering;

import org.cojen.tupl.diag.DatabaseStats;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RemoteUtils {
    private static final VarHandle cLongArrayElementHandle;

    static {
        try {
            cLongArrayElementHandle = MethodHandles.arrayElementVarHandle(long[].class);
        } catch (Throwable e) {
            throw new ExceptionInInitializerError();
        }
    }

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

    public static byte[] encodeTokens(long... tokens) {
        var bytes = new byte[4 + tokens.length * 8];
        Utils.encodeIntBE(bytes, 0, tokens.length);
        for (int i=0; i<tokens.length; i++) {
            var token = (long) cLongArrayElementHandle.getAcquire(tokens, i);
            Utils.encodeLongBE(bytes, 4 + i * 8, token);
        }
        return bytes;
    }

    public static boolean evalTokens(InputStream in, OutputStream out, long... expected)
        throws IOException
    {
        var din = new DataInputStream(in);
        int num = din.readInt();
        var bytes = new byte[num * 8];
        din.readFully(bytes);

        for (int i=0; i<num; i++) {
            long token = Utils.decodeLongBE(bytes, i * 8);

            for (long t : expected) {
                if (token == t) {
                    out.write(1);
                    return true;
                }
            }
        }

        return false;
    }
}
