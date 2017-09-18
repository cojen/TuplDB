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
class DecodingInputStream extends ByteArrayInputStream {
    DecodingInputStream(byte[] data) {
        super(data);
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
        int len = decodeIntLE();
        byte[] bytes = Arrays.copyOfRange(buf, pos, pos += len);
        return new String(bytes, StandardCharsets.UTF_8);
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
