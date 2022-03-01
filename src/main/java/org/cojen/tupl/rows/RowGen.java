/*
 *  Copyright 2021 Cojen.org
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

package org.cojen.tupl.rows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.cojen.maker.ClassMaker;

/**
 * Cache of objects used by code generation.
 *
 * @author Brian S O'Neill
 */
class RowGen {
    private static final Object MAKER_KEY = new Object(); // must be a private hidden instance

    final RowInfo info;

    private volatile String[] mStateFields;

    private volatile Map<String, Integer> mColumnNumbers;

    private volatile ColumnCodec[] mKeyCodecs;
    private volatile ColumnCodec[] mValueCodecs;

    private volatile Map<String, ColumnCodec> mKeyCodecMap;
    private volatile Map<String, ColumnCodec> mValueCodecMap;

    RowGen(RowInfo info) {
        this.info = info;
    }

    /**
     * @param who the class which is making a class (can be null)
     * @param rowType defines the ClassLoader to use (can be null)
     * @param suffix appended to class name (can be null)
     */
    public ClassMaker beginClassMaker(Class<?> who, Class<?> rowType, String suffix) {
        return beginClassMaker(who, rowType, info, null, suffix);
    }

    /**
     * @param who the class which is making a class (can be null)
     * @param rowType defines the ClassLoader to use (can be null)
     * @param subPackage optional (can be null)
     * @param suffix appended to class name (can be null)
     */
    public ClassMaker beginClassMaker(Class<?> who, Class<?> rowType,
                                      String subPackage, String suffix)
    {
        return beginClassMaker(who, rowType, info, subPackage, suffix);
    }

    /**
     * @param who the class which is making a class (can be null)
     * @param rowType defines the ClassLoader to use (can be null)
     * @param info used for defining the class name (can be null)
     * @param subPackage optional (can be null)
     * @param suffix appended to class name (can be null)
     */
    public static ClassMaker beginClassMaker(Class<?> who, Class<?> rowType, RowInfo info,
                                             String subPackage, String suffix)
    {
        final String name;

        String infoName;
        if (info == null || (infoName = info.name) == null) {
            name = null;
        } else {
            var bob = new StringBuilder();

            if (subPackage != null) {
                int ix = info.name.lastIndexOf('.');
                if (ix > 0) {
                    bob.append(info.name, 0, ix).append('.');
                    infoName = info.name.substring(ix + 1);
                }
                bob.append(subPackage).append('.');
            }

            bob.append(infoName);

            if (suffix != null && !suffix.isEmpty()) {
                bob.append('-').append(suffix);
            }

            name = bob.toString();
        }

        return beginClassMaker(who, rowType == null ? null : rowType.getClassLoader(), name);
    }

    /**
     * @param who the class which is making a class (can be null)
     */
    public static ClassMaker beginClassMaker(Class<?> who, ClassLoader loader, String name) {
        ClassMaker cm = ClassMaker.begin(name, loader, MAKER_KEY);

        if (who != null) {
            cm.sourceFile(who.getSimpleName());
        }

        var thisModule = RowGen.class.getModule();
        var thatModule = cm.classLoader().getUnnamedModule();

        // Generated code needs access to these non-exported packages.
        thisModule.addExports("org.cojen.tupl.core", thatModule);
        thisModule.addExports("org.cojen.tupl.filter", thatModule);
        thisModule.addExports("org.cojen.tupl.rows", thatModule);
        thisModule.addExports("org.cojen.tupl.views", thatModule);

        return cm;
    }

    /**
     * @param who the class which is making a class (can be null)
     * @param peer defines the package to define the new class in
     */
    public ClassMaker anotherClassMaker(Class<?> who, Class<?> peer, String suffix) {
        String name = info.name;
        int ix = name.lastIndexOf('.');
        if (ix > 0) {
            name = name.substring(ix + 1);
        }
        if (suffix != null && !suffix.isEmpty()) {
            name = name + '-' + suffix;
        }
        name = peer.getPackageName() + '.' + name;
        return beginClassMaker(who, peer.getClassLoader().getParent(), name);
    }

    /**
     * Returns the names of the generated state fields, each of which tracks up to 16 columns.
     */
    public String[] stateFields() {
        String[] fields = mStateFields;
        if (fields == null) {
            fields = makeStateFields();
        }
        return fields;
    }

    private String[] makeStateFields() {
        // One field per 16 columns (2 bits for each column).
        String[] fields = new String[(info.allColumns.size() + 15) / 16];

        for (int i=0; i<fields.length; i++) {
            fields[i] = unique("state$" + i);
        }

        mStateFields = fields;
        return fields;
    }

    /**
     * Returns a field name which doesn't clash with a column name. The given prefix must
     * already be unique among prefixes.
     */
    private String unique(String prefix) {
        String name = prefix;
        while (true) {
            if (!info.allColumns.containsKey(name)) {
                return name;
            }
            name += '$';
        }
    }

    /**
     * Returns a mapping of columns to zero-based column numbers. Keys are ordered
     * first. Within the two groups, column order matches encoding order.
     */
    public Map<String, Integer> columnNumbers() {
        Map<String, Integer> map = mColumnNumbers;
        if (map == null) {
            map = makeColumnNumbers();
        }
        return map;
    }

    private Map<String, Integer> makeColumnNumbers() {
        var map = new LinkedHashMap<String, Integer>(info.allColumns.size() << 1);

        // Keys first, to cluster their column states together.
        int num = 0;
        for (ColumnInfo ci : info.keyColumns.values()) {
            map.put(ci.name, num++);
        }
        for (ColumnCodec codec : valueCodecs()) { // use encoding order
            if (!(codec instanceof SchemaVersionColumnCodec)) {
                map.put(codec.mInfo.name, num++);
            }
        }

        mColumnNumbers = map;
        return map;
    }

    public String stateField(int columnNum) {
        return stateFields()[stateFieldNum(columnNum)];
    }

    public static int stateFieldNum(int columnNum) {
        return columnNum >> 4;
    }

    public static int stateFieldMask(int columnNum) {
        return stateFieldMask(columnNum, 0b11);
    }

    public static int stateFieldMask(int columnNum, int state) {
        return state << ((columnNum & 0b1111) << 1);
    }

    /**
     * Returns a new array consisting of the key codecs followed by the value codecs.
     */
    public ColumnCodec[] codecsCopy() {
        ColumnCodec[] keyCodecs = keyCodecs();
        ColumnCodec[] valueCodecs = valueCodecs();
        var all = new ColumnCodec[keyCodecs.length + valueCodecs.length];
        System.arraycopy(keyCodecs, 0, all, 0, keyCodecs.length);
        System.arraycopy(valueCodecs, 0, all, keyCodecs.length, valueCodecs.length);
        return all;
    }

    /**
     * Returns the key codecs in the order in which they should be encoded, which is the same
     * as RowInfo.keyColumns order (declaration order).
     */
    public ColumnCodec[] keyCodecs() {
        ColumnCodec[] codecs = mKeyCodecs;
        if (codecs == null) {
            mKeyCodecs = codecs = ColumnCodec.make(info.keyColumns, true);
        }
        return codecs;
    }

    /**
     * Returns the value codecs in the order in which they should be encoded, which can differ
     * from RowInfo.valueColumns order (lexicographical order).
     */
    public ColumnCodec[] valueCodecs() {
        ColumnCodec[] codecs = mValueCodecs;

        if (codecs != null) {
            return codecs;
        }

        var infos = new ArrayList<ColumnInfo>(info.valueColumns.values());

        if (!(info instanceof SecondaryInfo sInfo) || !sInfo.isAltKey()) {
            // Encode fixed sized columns first (primitives), then nullable primitives, and
            // then the remaining in natural order (lexicographical).

            infos.sort((a, b) -> {
                int ap = a.plainTypeCode();
                int bp = b.plainTypeCode();

                if (ColumnInfo.isPrimitive(ap)) {
                    if (!ColumnInfo.isPrimitive(bp)) {
                        return -1;
                    }
                    if (!a.isNullable()) {
                        if (b.isNullable()) {
                            return -1;
                        }
                    } else if (!b.isNullable()) {
                        return 1;
                    }
                } else if (ColumnInfo.isPrimitive(bp)) {
                    return 1;
                }

                return 0;
            });

            codecs = ColumnCodec.make(infos, false);
        } else {
            // For alternate keys, encode fixed sized columns first (primitives), then the same
            // order as the primary key columns, and then the remaining in natural order
            // (lexicographical). It's generally not expected that there will be any "remaining"
            // columns because alternate keys cannot currently be defined as covering indexes.
            // In other words, all value columns of an alternate key are part of the primary key.

            RowInfo primaryInfo = sInfo.primaryInfo;
            Map<String, ColumnInfo> primaryKeys = primaryInfo.keyColumns;

            infos.sort((a, b) -> {
                int ap = a.plainTypeCode();
                int bp = b.plainTypeCode();

                if (ColumnInfo.isPrimitive(ap)) {
                    if (!ColumnInfo.isPrimitive(bp)) {
                        return -1;
                    }
                } else if (ColumnInfo.isPrimitive(bp)) {
                    return 1;
                }

                if (primaryKeys.containsKey(a.name)) {
                    if (!primaryKeys.containsKey(b.name)) {
                        return -1;
                    }
                } else if (primaryKeys.containsKey(b.name)) {
                    return 1;
                }

                return 0;
            });

            // To reduce the cost of loading by primary key, alternate key columns should adopt
            // the same encoding format as primary key columns, when possible. This eliminates
            // extra conversion steps.

            var pkCodecs = new HashMap<String, ColumnCodec>();
            for (ColumnCodec codec : primaryInfo.rowGen().keyCodecs()) {
                pkCodecs.put(codec.mInfo.name, codec);
            }

            codecs = ColumnCodec.make(infos, pkCodecs);
        }

        return mValueCodecs = codecs;
    }

    /**
     * Returns a map of key codecs, in the order in which they should be encoded.
     */
    public Map<String, ColumnCodec> keyCodecMap() {
        var map = mKeyCodecMap;
        if (map == null) {
            mKeyCodecMap = map = makeCodecMap(keyCodecs());
        }
        return map;
    }

    /**
     * Returns a map of value codecs, in the order in which they should be encoded.
     */
    public Map<String, ColumnCodec> valueCodecMap() {
        var map = mValueCodecMap;
        if (map == null) {
            mValueCodecMap = map = makeCodecMap(valueCodecs());
        }
        return map;
    }

    private static Map<String, ColumnCodec> makeCodecMap(ColumnCodec[] codecs) {
        var map = new LinkedHashMap<String, ColumnCodec>(codecs.length);
        for (ColumnCodec codec : codecs) {
            map.put(codec.mInfo.name, codec);
        }
        return map;
    }
}
