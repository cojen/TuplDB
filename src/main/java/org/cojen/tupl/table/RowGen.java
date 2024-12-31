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

package org.cojen.tupl.table;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import java.util.function.IntFunction;
import java.util.function.Predicate;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.codec.ColumnCodec;
import org.cojen.tupl.table.codec.SchemaVersionColumnCodec;

/**
 * Cache of objects used by code generation.
 *
 * @author Brian S O'Neill
 */
public class RowGen {
    private static final Object MAKER_KEY = new Object();

    private static volatile long cPackageNum;
    private static final VarHandle cPackageNumHandle;

    static {
        try {
            cPackageNumHandle = MethodHandles.lookup().findStaticVarHandle
                (RowGen.class, "cPackageNum", long.class);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    /**
     * Returns a new sub-package name which can be used to facilitate class unloading.
     */
    public static String newSubPackage() {
        return "p" + (long) cPackageNumHandle.getAndAdd(1L);
    }

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
     * @param rowType defines the parent ClassLoader to use (can be null)
     * @param suffix appended to class name (can be null)
     */
    public ClassMaker beginClassMaker(Class<?> who, Class<?> rowType, String suffix) {
        return beginClassMaker(who, rowType, info, null, suffix);
    }

    /**
     * @param who the class which is making a class (can be null)
     * @param rowType defines the parent ClassLoader to use (can be null)
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
     * @param rowType defines the parent ClassLoader to use (can be null)
     * @param info used for defining the class name (can be null)
     * @param subPackage optional (can be null)
     * @param suffix appended to class name (can be null)
     */
    public static ClassMaker beginClassMaker(Class<?> who, Class<?> rowType, RowInfo info,
                                             String subPackage, String suffix)
    {
        return beginClassMaker(who, rowType, info == null ? null : info.name, subPackage, suffix);
    }

    /**
     * @param who the class which is making a class (can be null)
     * @param rowType defines the parent ClassLoader to use (can be null)
     * @param infoName row info name (can be null)
     * @param subPackage optional (can be null)
     * @param suffix appended to class name (can be null)
     */
    public static ClassMaker beginClassMaker(Class<?> who, Class<?> rowType, String infoName,
                                             String subPackage, String suffix)
    {
        final String name;

        if (infoName == null) {
            name = null;
        } else {
            var bob = new StringBuilder();

            if (subPackage != null) {
                int ix = infoName.lastIndexOf('.');
                if (ix > 0) {
                    bob.append(infoName, 0, ix).append('.');
                    infoName = infoName.substring(ix + 1);
                }
                bob.append(subPackage).append('.');
            }

            bob.append(infoName);

            if (suffix != null && !suffix.isEmpty()) {
                bob.append('-').append(suffix);
            }

            name = bob.toString();
        }

        ClassLoader parentLoader;

        if (rowType == null) {
            parentLoader = null;
        } else {
            parentLoader = rowType.getClassLoader();

            if (rowType.isSynthetic()) {
                // Assume it was generated using beginClassMakerForRowType, so choose its
                // parent loader in order for the new class to be generated into the same
                // loader. This isn't strictly required -- it just reduces the number of
                // ClassLoader instances.
                parentLoader = parentLoader.getParent();
            }
        }

        return beginClassMaker(who, parentLoader, name);
    }

    /**
     * @param who the class which is making a class (can be null)
     */
    public static ClassMaker beginClassMaker(Class<?> who, ClassLoader parentLoader, String name) {
        ClassMaker cm = ClassMaker.begin(name, parentLoader, MAKER_KEY);

        if (who != null) {
            cm.sourceFile(who.getSimpleName());
        }

        var thisModule = RowGen.class.getModule();
        var thatModule = cm.classLoader().getUnnamedModule();

        // Generated code needs access to these non-exported packages.
        thisModule.addExports("org.cojen.tupl.core", thatModule);
        thisModule.addExports("org.cojen.tupl.table", thatModule);
        thisModule.addExports("org.cojen.tupl.table.filter", thatModule);
        thisModule.addExports("org.cojen.tupl.views", thatModule);

        return cm;
    }

    /**
     * Returns a ClassMaker suitable for defining a new row type interface. The fully qualified
     * class name includes a generated sub package to facilitate class unloading. For example:
     *
     *     basePackage = "org.cojen.tupl"
     *     typeName    = "app.MyTable"
     *
     *  ...can yield a class name like: "org.cojen.tupl.p0.app.MyTable-1"
     */
    public static ClassMaker beginClassMakerForRowType(String basePackage, String typeName) {
        String name = basePackage + '.' + newSubPackage() + '.' + typeName;

        ClassMaker cm = ClassMaker.begin(name, null, MAKER_KEY);
        cm.public_().interface_();

        // Make synthetic in order for new classes generated via beginClassMaker to use the
        // same ClassLoader as the row type interface.
        cm.synthetic();

        return cm;
    }

    /**
     * @param who the class which is making a class (can be null)
     * @param peer defines the package to define the new class in
     * @param suffix appended to class name (can be null)
     */
    public ClassMaker anotherClassMaker(Class<?> who, Class<?> peer, String suffix) {
        return anotherClassMaker(who, info.name, peer, suffix);
    }

    /**
     * @param who the class which is making a class (can be null)
     * @param infoName non-null row info name
     * @param peer defines the package to define the new class in
     * @param suffix appended to class name (can be null)
     */
    public static ClassMaker anotherClassMaker(Class<?> who, String infoName,
                                               Class<?> peer, String suffix)
    {
        String name = infoName;
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
                map.put(codec.info.name, num++);
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
        return state << stateFieldShift(columnNum);
    }

    public static int stateFieldShift(int columnNum) {
        return (columnNum & 0b1111) << 1;
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
            mKeyCodecs = codecs = ColumnCodec.make(info.keyColumns, ColumnCodec.F_LEX);
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

            codecs = ColumnCodec.make(infos, 0);
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
                pkCodecs.put(codec.info.name, codec);
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
            map.put(codec.info.name, codec);
        }
        return map;
    }

    /**
     * Generates code which checks if all of the given columns are set for a given row object,
     * returning true or false.
     *
     * @param rowVar variable which references a row with state fields
     */
    void checkSet(MethodMaker mm, Map<String, ColumnInfo> columns, Variable rowVar) {
        checkSet(mm, columns, null, num -> rowVar.field(stateField(num)));
    }

    /**
     * Generates code which checks if all of the given columns are set, returning true or
     * false.
     *
     * @param resultVar pass null to return; pass boolean var to set instead
     * @param stateFieldAccessor returns a state field for a given zero-based column number
     * @see #stateField
     */
    void checkSet(MethodMaker mm, Map<String, ColumnInfo> columns,
                  Variable resultVar, IntFunction<Variable> stateFieldAccessor)
    {
        if (columns.isEmpty()) {
            if (resultVar == null) {
                mm.return_(true);
            } else {
                resultVar.set(true);
            }
            return;
        }

        if (columns.size() == 1) {
            int num = columnNumbers().get(columns.values().iterator().next().name);
            var retVar = stateFieldAccessor.apply(num).and(stateFieldMask(num)).ne(0);
            if (resultVar == null) {
                mm.return_(retVar);
            } else {
                resultVar.set(retVar);
            }
            return;
        }

        Label end = resultVar == null ? null : mm.label();

        int num = 0, mask = 0;

        for (int step = 0; step < 2; step++) {
            // Key columns are numbered before value columns. Add checks in two steps.
            // Note that the codecs are accessed, to match encoding order.
            var baseCodecs = step == 0 ? keyCodecs() : valueCodecs();

            for (ColumnCodec codec : baseCodecs) {
                ColumnInfo info = codec.info;

                if (columns.containsKey(info.name)) {
                    mask |= stateFieldMask(num);
                }

                if (isMaskReady(++num, mask)) {
                    // Convert all states of value 0b01 (clean) into value 0b11 (dirty). All
                    // other states stay the same.
                    var state = stateFieldAccessor.apply(num - 1).get();
                    state = state.or(state.and(0x5555_5555).shl(1));

                    // Flip all column state bits. If final result is non-zero, then some
                    // columns were unset.
                    state = state.xor(mask);
                    mask = maskRemainder(num, mask);
                    if (mask != 0xffff_ffff) {
                        state = state.and(mask);
                    }

                    Label cont = mm.label();
                    state.ifEq(0, cont);

                    if (resultVar == null) {
                        mm.return_(false);
                    } else {
                        resultVar.set(false);
                        end.goto_();
                    }

                    cont.here();
                    mask = 0;
                }
            }
        }

        if (resultVar == null) {
            mm.return_(true);
        } else {
            resultVar.set(true);
            end.here();
        }
    }

    /**
     * Generates code which checks if at least one of the given columns is set for a given row
     * object, returning true or false.
     *
     * @param resultVar pass null to return; pass boolean var to set instead
     * @param rowVar variable which references a row with state fields
     */
    void checkAnySet(MethodMaker mm, Map<String, ColumnInfo> columns,
                     Variable resultVar, Variable rowVar)
    {
        checkAnySet(mm, columns, resultVar, num -> rowVar.field(stateField(num)));
    }

    /**
     * Generates code which checks if at least one of the given columns is set, returning true
     * or false.
     *
     * @param resultVar pass null to return; pass boolean var to set instead
     * @param stateFieldAccessor returns a state field for a given zero-based column number
     * @see #stateField
     */
    void checkAnySet(MethodMaker mm, Map<String, ColumnInfo> columns,
                     Variable resultVar, IntFunction<Variable> stateFieldAccessor)
    {
        checkAny(mm, columns, resultVar, stateFieldAccessor, 0b11);
    }

    /**
     * Generates code which checks if all of the given columns are dirty for a given row object,
     * returning true or false.
     *
     * @param rowVar variable which references a row with state fields
     */
    void checkDirty(MethodMaker mm, Map<String, ColumnInfo> columns, Variable rowVar) {
        checkDirty(mm, columns, null, num -> rowVar.field(stateField(num)));
    }

    /**
     * Generates code which checks if all of the given columns are dirty, returning true or
     * false.
     *
     * @param resultVar pass null to return; pass boolean var to set instead
     * @param stateFieldAccessor returns a state field for a given zero-based column number
     * @see #stateField
     */
    void checkDirty(MethodMaker mm, Map<String, ColumnInfo> columns,
                    Variable resultVar, IntFunction<Variable> stateFieldAccessor)
    {
        Label end = resultVar == null ? null : mm.label();

        int num = 0, mask = 0;

        for (int step = 0; step < 2; step++) {
            // Key columns are numbered before value columns. Add checks in two steps.
            // Note that the codecs are accessed, to match encoding order.
            var baseCodecs = step == 0 ? keyCodecs() : valueCodecs();

            for (ColumnCodec codec : baseCodecs) {
                ColumnInfo info = codec.info;

                if (columns.containsKey(info.name)) {
                    mask |= stateFieldMask(num);
                }

                if (isMaskReady(++num, mask)) {
                    Label cont = mm.label();
                    stateFieldAccessor.apply(num - 1).and(mask).ifEq(mask, cont);

                    if (resultVar == null) {
                        mm.return_(false);
                    } else {
                        resultVar.set(false);
                        end.goto_();
                    }

                    cont.here();
                    mask = 0;
                }
            }
        }

        if (resultVar == null) {
            mm.return_(true);
        } else {
            resultVar.set(true);
            end.here();
        }
    }

    /**
     * Generates code which checks if at least one of the given columns are dirty for a given
     * row object, returning true or false.
     *
     * @param rowVar variable which references a row with state fields
     */
    void checkAnyDirty(MethodMaker mm, Map<String, ColumnInfo> columns, Variable rowVar) {
        checkAnyDirty(mm, columns, null, num -> rowVar.field(stateField(num)));
    }

    /**
     * Generates code which checks if at least one of the given columns are dirty, returning
     * true or false.
     *
     * @param resultVar pass null to return; pass boolean var to set instead
     * @param stateFieldAccessor returns a state field for a given zero-based column number
     * @see #stateField
     */
    void checkAnyDirty(MethodMaker mm, Map<String, ColumnInfo> columns,
                       Variable resultVar, IntFunction<Variable> stateFieldAccessor)
    {
        checkAny(mm, columns, resultVar, stateFieldAccessor, 0b10);
    }

    private void checkAny(MethodMaker mm, Map<String, ColumnInfo> columns,
                          Variable resultVar, IntFunction<Variable> stateFieldAccessor,
                          int state)
    {
        Label end = resultVar == null ? null : mm.label();

        int num = 0, mask = 0;

        for (int step = 0; step < 2; step++) {
            // Key columns are numbered before value columns. Add checks in two steps.
            // Note that the codecs are accessed, to match encoding order.
            var baseCodecs = step == 0 ? keyCodecs() : valueCodecs();

            for (ColumnCodec codec : baseCodecs) {
                ColumnInfo info = codec.info;

                if (columns.containsKey(info.name)) {
                    mask |= stateFieldMask(num, state);
                }

                if (isMaskReady(++num, mask)) {
                    Label cont = mm.label();
                    stateFieldAccessor.apply(num - 1).and(mask).ifEq(0, cont);

                    if (resultVar == null) {
                        mm.return_(true);
                    } else {
                        resultVar.set(true);
                        end.goto_();
                    }

                    cont.here();
                    mask = 0;
                }
            }
        }

        if (resultVar == null) {
            mm.return_(false);
        } else {
            resultVar.set(false);
            end.here();
        }
    }

    /**
     * Generates code which accesses a row and always throws a detailed exception describing
     * the required columns which aren't set. A check method should have been invoked first.
     *
     * @param rowVar variable which references a row with state fields
     */
    void requireSet(MethodMaker mm, Map<String, ColumnInfo> columns, Variable rowVar) {
        requireSet(mm, columns, num -> rowVar.field(stateField(num)));
    }

    /**
     * Generates code which accesses state fields and always throws a detailed exception
     * describing the required columns which aren't set. A check method should have been
     * invoked first.
     *
     * @param stateFieldAccessor returns a state field for a given zero-based column number
     * @see #stateField
     */
    void requireSet(MethodMaker mm, Map<String, ColumnInfo> columns,
                    IntFunction<Variable> stateFieldAccessor)
    {
        String initMessage = "Some required columns are unset";

        if (columns.isEmpty()) {
            mm.new_(IllegalStateException.class, initMessage).throw_();
            return;
        }

        int initLength = initMessage.length() + 2;
        var bob = mm.new_(StringBuilder.class, initLength << 1)
            .invoke("append", initMessage).invoke("append", ": ");

        boolean first = true;
        for (ColumnInfo info : columns.values()) {
            int num = columnNumbers().get(info.name);
            Label isSet = mm.label();
            stateFieldAccessor.apply(num).and(stateFieldMask(num)).ifNe(0, isSet);
            if (!first) {
                Label sep = mm.label();
                bob.invoke("length").ifEq(initLength, sep);
                bob.invoke("append", ", ");
                sep.here();
            }
            bob.invoke("append", info.name);
            isSet.here();
            first = false;
        }

        mm.new_(IllegalStateException.class, bob.invoke("toString")).throw_();
    }

    /**
     * Mark all the value columns as UNSET without modifying the primary key column states.
     */
    public void markNonPrimaryKeyColumnsUnset(Variable rowVar) {
        // Clear the value column state fields. Skip the key columns, which are numbered
        // first. Note that the codecs are accessed, to match encoding order.
        int num = info.keyColumns.size();
        int mask = 0;
        for (ColumnCodec codec : valueCodecs()) {
            mask |= stateFieldMask(num);
            if (isMaskReady(++num, mask)) {
                mask = maskRemainder(num, mask);
                Variable field = rowVar.field(stateField(num - 1));
                mask = ~mask;
                if (mask == 0) {
                    field.set(mask);
                } else {
                    field.set(field.and(mask));
                    mask = 0;
                }
            }
        }
    }

    /**
     * @param unsetPredicate returns true if the given column name should be unset
     */
    public void markUnset(Variable rowVar, Predicate<String> unsetPredicate) {
        int num = 0, mask = 0;

        for (int step = 0; step < 2; step++) {
            // Key columns are numbered before value columns. Add checks in two steps.
            // Note that the codecs are accessed, to match encoding order.
            var baseCodecs = step == 0 ? keyCodecs() : valueCodecs();

            for (ColumnCodec codec : baseCodecs) {
                if (unsetPredicate.test(codec.info.name)) {
                    mask |= stateFieldMask(num);
                }
                if (isMaskReady(++num, mask)) {
                    mask = maskRemainder(num, mask);
                    Variable field = rowVar.field(stateField(num - 1));
                    mask = ~mask;
                    if (mask == 0) {
                        field.set(mask);
                    } else {
                        field.set(field.and(mask));
                        mask = 0;
                    }
                }
            }
        }
    }

    /**
     * Marks all the given columns unset, and clear those columns which refer to objects.
     *
     * @param unsetPredicate returns true if the given column name should be unset
     */
    public void unsetAndClear(Variable rowVar, Predicate<String> unsetPredicate) {
        markUnset(rowVar, unsetPredicate);
        clearColumns(rowVar, unsetPredicate);
    }

    /**
     * Marks all the given columns unset, and clear those columns which refer to objects.
     */
    public void unsetAndClear(Variable rowVar, Map<String, ColumnInfo> columns) {
        unsetAndClear(rowVar, columns::containsKey);
    }

    /**
     * Clears columns which refer to objects.
     *
     * @param clearPredicate returns true if the given column name should be cleared
     */
    public void clearColumns(Variable rowVar, Predicate<String> clearPredicate) {
        for (ColumnInfo target : info.allColumns.values()) {
            String name = target.name;
            if (!target.type.isPrimitive() && clearPredicate.test(name)) {
                rowVar.field(name).set(null);
            }
        }
    }

    /**
     * Called when building state field masks for columns, when iterating them in order.
     *
     * @param num column number pre-incremented to the next one
     * @param mask current group; must be non-zero to have any effect
     */
    private boolean isMaskReady(int num, int mask) {
        return mask != 0 && ((num & 0b1111) == 0 || num >= info.allColumns.size());
    }

    /**
     * When building a mask for the highest state field, sets the high unused bits on the
     * mask. This can eliminate an unnecessary 'and' operation.
     *
     * @param num column number pre-incremented to the next one
     * @param mask current group
     * @return updated mask
     */
    private int maskRemainder(int num, int mask) {
        if (num >= info.allColumns.size()) {
            int shift = (num & 0b1111) << 1;
            if (shift != 0) {
                mask |= 0xffff_ffff << shift;
            }
        }
        return mask;
    }
}
