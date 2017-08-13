/*
 *  Copyright (C) 2017 Cojen.org
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

package org.cojen.tupl.repl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.Reader;

import java.nio.charset.StandardCharsets;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class OptionsDecoder extends ByteArrayInputStream {
    private Reader mReader;

    OptionsDecoder(InputStream in) throws IOException {
        super(decode(in));
    }

    private static byte[] decode(InputStream in) throws IOException {
        byte[] buf = new byte[4];
        Utils.readFully(in, buf, 0, buf.length);
        buf = new byte[Utils.decodeIntLE(buf, 0) - 4];
        Utils.readFully(in, buf, 0, buf.length);
        return buf;
    }

    public int decodeIntLE() {
        int value = Utils.decodeIntLE(buf, pos);
        pos += 4;
        return value;
    }

    public long decodeLongLE() {
        long value = Utils.decodeLongLE(buf, pos);
        pos += 8;
        return value;
    }

    public String decodeStr() {
        Reader reader = mReader;
        if (reader == null) {
            mReader = reader = new InputStreamReader(this);
        }
        try {
            char[] chars = new char[decodeIntLE()];
            reader.read(chars);
            return new String(chars);
        } catch (IOException e) {
            // Not expected.
            throw Utils.rethrow(e);
        }
    }

    public Map<String, String> decodeMap() {
        int size = decodeIntLE();
        if (size == 0) {
            return Collections.emptyMap();
        }
        Map<String, String> map = new HashMap<>();
        while (--size >= 0) {
            map.put(decodeStr(), decodeStr());
        }
        return map;
    }
}
