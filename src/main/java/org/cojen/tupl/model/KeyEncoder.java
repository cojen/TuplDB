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

package org.cojen.tupl.model;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.nio.ByteOrder;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.cojen.tupl.core.Tuple;
import org.cojen.tupl.core.Utils;

/**
 * Supports encoding of complex cache keys. The format resembles something used by object
 * serialization, except it's not pure -- the finished key can reference object instances.
 *
 * @author Brian S. O'Neill
 * @see Node#encodeKey
 * @see #finish
 */
final class KeyEncoder {
    private static byte cLastType;

    static synchronized byte allocType() {
        byte type = (byte) (cLastType + 1);
        if (type == 0) {
            throw new IllegalStateException();
        }
        cLastType = type;
        return type;
    }

    private static final byte ENTITY_REF = allocType(), OBJECT_REF = allocType();

    static final VarHandle cShortArrayHandle;
    static final VarHandle cIntArrayHandle;
    static final VarHandle cLongArrayHandle;

    static {
        try {
            cShortArrayHandle = MethodHandles.byteArrayViewVarHandle
                (short[].class, ByteOrder.LITTLE_ENDIAN);
            cIntArrayHandle = MethodHandles.byteArrayViewVarHandle
                (int[].class, ByteOrder.LITTLE_ENDIAN);
            cLongArrayHandle = MethodHandles.byteArrayViewVarHandle
                (long[].class, ByteOrder.LITTLE_ENDIAN);
        } catch (Throwable e) {
            throw new ExceptionInInitializerError();
        }
    }

    private byte[] mBuffer;
    private int mSize;

    private final HashMap<Object, Integer> mEntityMap;
    private final LinkedHashMap<Object, Integer> mObjectMap;

    KeyEncoder() {
        mBuffer = new byte[16];
        mEntityMap = new HashMap<>();
        mObjectMap = new LinkedHashMap<>();
    }

    /**
     * Call to begin encoding the entity, which is expected to be a Node or Type. Only continue
     * encoding when true is returned. A return of false indicates that the entity has already
     * been encoded.
     *
     * @param type constant obtained by calling allocType
     * @return false if the entity has already been encoded
     */
    boolean encode(Object entity, byte type) {
        Integer id = mEntityMap.putIfAbsent(entity, mEntityMap.size());
        if (id == null) {
            encodeType(type);
            return true;
        } else {
            encodeType(ENTITY_REF);
            encodeUnsignedVarInt(id);
            return false;
        }
    }

    /**
     * Only call this method directly if the entity being encoded is always just one byte. Call
     * the encode method for all other cases.
     *
     * @param type constant obtained by calling allocType
     */
    void encodeType(byte type) {
        encodeByte(type);
    }

    void encodeBoolean(boolean b) {
        encodeByte(b ? 1 : 0);
    }

    void encodeByte(int n) {
        ensureCapacity(1);
        mBuffer[mSize++] = (byte) n;
    }

    void encodeShort(int n) {
        ensureCapacity(2);
        cShortArrayHandle.set(mBuffer, mSize, (short) n);
        mSize += 2;
    }

    void encodeInt(int n) {
        ensureCapacity(4);
        cIntArrayHandle.set(mBuffer, mSize, n);
        mSize += 4;
    }

    void encodeLong(long n) {
        ensureCapacity(8);
        cLongArrayHandle.set(mBuffer, mSize, n);
        mSize += 8;
    }

    void encodeUnsignedVarInt(int n) {
        ensureCapacity(Utils.calcUnsignedVarIntLength(n));
        Utils.encodeUnsignedVarInt(mBuffer, mSize, n);
    }

    // encode varint length, followed by bytes
    //void encodeUTF(String str);

    void encodeString(String str) {
        if (str != null) {
            // Ccache keys are expected to live a long time, and many strings are expected to
            // have the same value. Call intern to reduce the duplication.
            str = str.intern();
        }
        encodeObject(str);
    }

    void encodeClass(Class clazz) {
        encodeObject(clazz);
    }

    void encodeObject(Object obj) {
        Integer id = mObjectMap.size();
        Integer existing = mObjectMap.putIfAbsent(obj, id);
        if (existing != null) {
            id = existing;
        }
        encodeType(OBJECT_REF);
        encodeUnsignedVarInt(id);
    }

    void encodeOptionalNode(Node node) {
        if (node == null) {
            encodeBoolean(false);
        } else {
            encodeBoolean(true);
            node.encodeKey(this);
        }
    }

    void encodeInts(int[] ints) {
        if (ints == null) {
            encodeByte(0);
        } else {
            encodeUnsignedVarInt(ints.length + 1);
            for (int i : ints) {
                encodeInt(i);
            }
        }
    }

    void encodeStrings(String[] strs) {
        if (strs == null) {
            encodeByte(0);
        } else {
            encodeUnsignedVarInt(strs.length + 1);
            for (String s : strs) {
                encodeString(s);
            }
        }
    }

    void encodeNodes(Node[] nodes) {
        if (nodes == null) {
            encodeByte(0);
        } else {
            encodeUnsignedVarInt(nodes.length + 1);
            for (Node n : nodes) {
                n.encodeKey(this);
            }
        }
    }

    void encodeNodes(Collection<? extends Node> nodes) {
        if (nodes == null) {
            encodeByte(0);
        } else {
            encodeUnsignedVarInt(nodes.size() + 1);
            for (Node n : nodes) {
                n.encodeKey(this);
            }
        }
    }

    /**
     * Returns a cache key instance.
     */
    Object finish() {
        byte[] bytes = mBuffer;
        if (mSize < bytes.length) {
            bytes = Arrays.copyOf(mBuffer, mSize);
        }

        int numObjects = mObjectMap.size();

        if (numObjects == 0) {
            return Tuple.make.with(bytes);
        }

        if (numObjects == 1) {
            return Tuple.make.with(bytes, mObjectMap.keySet().iterator().next());
        }

        return Tuple.make.with(bytes, mObjectMap.keySet().toArray(new Object[numObjects]));
    }

    private void ensureCapacity(int amt) {
        if (mSize + amt > mBuffer.length) {
            expand(amt);
        }
    }

    private void expand(int amt) {
        mBuffer = Arrays.copyOf(mBuffer, Math.max(mSize + amt, mSize << 1));
    }
}
