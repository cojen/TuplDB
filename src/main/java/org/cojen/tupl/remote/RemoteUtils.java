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
             DeadlockExceptionSerializer.THE,
             QueryExceptionSerializer.THE);

        return env;
    }

    private static final long MAGIC_NUMBER = 2825672906279293275L, GROUP_ID = 5156919750013540996L;

    private static final int HEADER_SIZE = 44;

    /**
     * Encodes a header which follows the repl.ChannelManager format.
     */
    public static byte[] encodeConnectHeader(long... tokens) {
        if (tokens.length < 1 || tokens.length > 2) {
            throw new IllegalArgumentException("Must provide one or two tokens");
        }

        var header = new byte[HEADER_SIZE];

        Utils.encodeLongLE(header, 0, MAGIC_NUMBER);
        Utils.encodeLongLE(header, 8, GROUP_ID); // synthetic group identifier

        for (int i=0; i<tokens.length; i++) {
            var token = (long) cLongArrayElementHandle.getAcquire(tokens, i);
            Utils.encodeLongLE(header, 28 + i * 8, token);
        }

        return header;
    }

    /**
     * @param out must be null for client side
     * @return false if rejected
     */
    public static boolean testConnection(InputStream in, OutputStream out, long... tokens)
        throws IOException
    {
        var header = new byte[HEADER_SIZE];
        if (in.readNBytes(header, 0, header.length) < header.length) {
            return false;
        }

        if (Utils.decodeLongLE(header, 0) != MAGIC_NUMBER ||
            Utils.decodeLongLE(header, 8) != GROUP_ID)
        {
            return false;
        }

        boolean passed = false;

        testTokens: for (int i=0; i<2; i++) {
            long provided = Utils.decodeLongLE(header, 28 + i * 8);
            for (int j=0; j<tokens.length; j++) {
                var token = (long) cLongArrayElementHandle.getAcquire(tokens, j);
                if (provided == token) {
                    passed = true;
                    break testTokens;
                }
            }
        }

        if (out != null) {
            if (!passed) {
                // Use illegal identifier (0) to indicate that token doesn't match.
                Utils.encodeLongLE(header, 8, 0);
            }
            out.write(header);
        }

        return passed;
    }
}
