/*
 *  Copyright (C) 2021 Cojen.org
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

package org.cojen.tupl.table.codec;

import java.lang.reflect.Modifier;

import org.cojen.maker.FieldMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.CompareUtils;
import org.cojen.tupl.table.ConvertUtils;
import org.cojen.tupl.table.Converter;

import org.cojen.tupl.table.filter.ColumnFilter;

/**
 * Abstract class for encoding and decoding columns that can be encoded as a variable length
 * string of bytes.
 *
 * @author Brian S O'Neill
 */
abstract class BytesColumnCodec extends ColumnCodec {
    /**
     * @param info non-null
     * @param mm is null for stateless instance
     */
    BytesColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    public int minSize() {
        return 0;
    }

    /**
     * Defines a byte[] arg field set to null or an encoded string of bytes. If an 'in' op,
     * then the field is a byte[][].
     */
    @Override
    public void filterDefineExtraFields(boolean in, Variable argVar, String argFieldName) {
        Class<?> fieldType = in ? byte[][].class : byte[].class;
        String fieldName = argFieldName(argFieldName, bytesFieldSuffix());

        FieldMaker fm;
        try {
            fm = defineArgField(fieldType, fieldName);
        } catch (IllegalStateException e) {
            // Already defined. Assume this is okay and that the string returned by
            // bytesFieldSuffix() ensures that unique encodings have unique field names.
            return;
        }

        if (argVar != null) { 
            fm.final_();
            maker.field(fieldName).set(filterPrepareBytes(argVar, in));
            return;
        }

        // Define a method for accessing the field, which lazily initializes it.

        MethodMaker mm = maker.classMaker().addMethod(byte[].class, fieldName);
        var codec = (BytesColumnCodec) bind(mm);

        var bytes = mm.field(fieldName).getOpaque();
        Label doInit = mm.label();
        bytes.ifEq(null, doInit);
        mm.return_(bytes);

        doInit.here();
        bytes.set(codec.filterPrepareBytes(mm.field(argFieldName), in));
        mm.field(fieldName).setOpaque(bytes);
        mm.return_(bytes);
    }

    private Variable filterPrepareBytes(Variable argVar, boolean in) {
        if (in) {
            var lengthVar = argVar.alength();
            return ConvertUtils.convertArray(maker, byte[][].class, lengthVar, ixVar -> {
                return filterPrepareBytes(argVar.aget(ixVar));
            });
        } else {
            return filterPrepareBytes(argVar);
        }
    }

    /**
     * Called by filterDefineExtraFields.
     *
     * @return byte[] variable
     */
    protected abstract Variable filterPrepareBytes(Variable argVar);

    /**
     * Returns true if the prepared byte array is suitable for unsigned comparison.
     */
    protected abstract boolean compareBytesUnsigned();

    @Override
    public boolean canFilterQuick(ColumnInfo dstInfo) {
        return dstInfo.unorderedTypeCode() == info.unorderedTypeCode();
    }

    @Override
    public Object filterQuickDecode(ColumnInfo dstInfo,
                                    Variable srcVar, Variable offsetVar, Variable endVar)
    {
        Variable lengthVar = maker.var(int.class);
        Variable isNullVar = info.isNullable() ? maker.var(boolean.class) : null;

        decodeHeader(srcVar, offsetVar, endVar, lengthVar, isNullVar);

        Variable dataOffsetVar = offsetVar.get(); // need a stable copy
        offsetVar.inc(lengthVar);

        return new Variable[] {dataOffsetVar, lengthVar, isNullVar};
    }


    /**
     * Called by filterQuickDecode to decode the string header and advance the offset to the
     * start of the string data.
     *
     * @param srcVar source byte array
     * @param offsetVar int type; is incremented as a side-effect
     * @param endVar end offset, which when null implies the end of the array
     * @param lengthVar set to the decoded length; must be definitely assigned
     * @param isNullVar set to true/false if applicable; must be definitely assigned for
     * nullable strings
     */
    protected abstract void decodeHeader(Variable srcVar, Variable offsetVar, Variable endVar,
                                         Variable lengthVar, Variable isNullVar);

    /**
     * Called by implementations of decodeHeader which add one to the length for encoding null.
     *
     * @param lengthVar already decoded but not yet adjusted
     * @param isNullVar is definitely assigned
     */
    protected void decodeNullableLength(Variable lengthVar, Variable isNullVar) {
        // Actual length is encoded plus one, and zero means null.
        Label notNull = maker.label();
        lengthVar.ifNe(0, notNull);
        isNullVar.set(true);
        Label cont = maker.label().goto_();
        notNull.here();
        isNullVar.set(false);
        lengthVar.inc(-1);
        cont.here();
    }

    @Override
    public void filterQuickCompare(ColumnInfo dstInfo, Variable srcVar, Variable offsetVar,
                                   int op, Object decoded, Variable argObjVar, int argNum,
                                   Label pass, Label fail)
    {
        var decodedVars = (Variable[]) decoded;

        if (!dstInfo.isNullable() && info.isNullable()) {
            Label cont = maker.label();
            Variable isNullVar = decodedVars[2];
            isNullVar.ifFalse(cont);
            var columnVar = maker.var(dstInfo.type);
            Converter.setDefault(maker, dstInfo, columnVar);
            var argField = argObjVar.field(argFieldName(argNum));
            CompareUtils.compare(maker, dstInfo, columnVar, dstInfo, argField, op, pass, fail);
            cont.here();
        }

        var argVar = bytesField(argObjVar, argNum);

        if (ColumnFilter.isIn(op)) {
            CompareUtils.compareIn(maker, argVar, op, pass, fail, (a, p, f) -> {
                compareQuickElement(srcVar, ColumnFilter.OP_EQ, decodedVars, a, p, f);
            });
        } else {
            compareQuickElement(srcVar, op, decodedVars, argVar, pass, fail);
        }
    }

    /**
     * @param decodedVars dataOffsetVar, lengthVar, and optional isNullVar
     * @param argVar byte array
     */
    private void compareQuickElement(Variable srcVar,
                                     int op, Variable[] decodedVars, Variable argVar,
                                     Label pass, Label fail)
    {
        Variable dataOffsetVar = decodedVars[0];
        Variable lengthVar = decodedVars[1];
        Variable isNullVar = decodedVars[2];

        Label notNull = maker.label();
        argVar.ifNe(null, notNull);
        // Argument is null...
        Label mismatch = CompareUtils.selectColumnToNullArg(info, op, pass, fail);
        if (isNullVar != null) {
            Label match = CompareUtils.selectNullColumnToNullArg(op, pass, fail);
            if (match != mismatch) {
                isNullVar.ifTrue(match);
            }
        }
        maker.goto_(mismatch);

        // Argument isn't null...
        notNull.here();
        if (isNullVar != null) {
            isNullVar.ifTrue(CompareUtils.selectNullColumnToArg(info, op, pass, fail));
        }

        CompareUtils.compareArrays(maker, compareBytesUnsigned(),
                                   srcVar, dataOffsetVar, dataOffsetVar.add(lengthVar),
                                   argVar, 0, argVar.alength(),
                                   op, pass, fail);
    }

    /**
     * Implementation of filterQuickCompare method for lexicographically ordered encodings.
     * Doesn't perform any special null handling because that must be encoded directly in the
     * bytes. Supports descending keys.
     *
     * @param dstInfo current definition for column
     * @param srcVar source byte array
     * @param offsetVar int type; must not be modified
     * @param endVar non-null end offset
     * @param op defined in ColumnFilter
     * @param argObjVar object which contains fields prepared earlier
     * @param argNum zero-based filter argument number
     * @param pass branch here when comparison passes
     * @param fail branch here when comparison fails
     */
    protected final void filterQuickCompareLex(ColumnInfo dstInfo,
                                               Variable srcVar, Variable offsetVar, Variable endVar,
                                               int op, Variable argObjVar, int argNum,
                                               Label pass, Label fail)
    {
        var argVar = bytesField(argObjVar, argNum);

        if (dstInfo.isDescending()) {
            op = ColumnFilter.reverseOperator(op);
        }

        if (ColumnFilter.isIn(op)) {
            CompareUtils.compareIn(maker, argVar, op, pass, fail, (a, p, f) -> {
                    CompareUtils.compareArrays(maker, true,
                                           srcVar, offsetVar, endVar,
                                           a, 0, a.alength(),
                                           ColumnFilter.OP_EQ, p, f);
            });
        } else {
            CompareUtils.compareArrays(maker, true,
                                       srcVar, offsetVar, endVar,
                                       argVar, 0, argVar.alength(),
                                       op, pass, fail);
        }
    }

    /**
     * @param argObjVar object which contains fields prepared earlier
     * @param argNum zero-based filter argument number
     */
    private Variable bytesField(Variable argObjVar, int argNum) {
        String name = argFieldName(argNum, bytesFieldSuffix());

        boolean isFinal = true;
        try {
            isFinal = Modifier.isFinal(argObjVar.classType().getDeclaredField(name).getModifiers());
        } catch (NoSuchFieldException e) {
            // Accessing the field below will throw an exception.
        }

        if (isFinal) {
            return argObjVar.field(name).get();
        } else {
            // Invoke the lazy init method.
            return argObjVar.invoke(name);
        }
    }

    /**
     * Should return a unique string among all the subclasses. This allows extra fields to be
     * shared when filtering against a secondary index and the column must be decoded against
     * the secondary and the primary. If this method always returned the same string for all
     * subclasses, then this might create field name conflicts.
     */
    private String bytesFieldSuffix() {
        String name = getClass().getSimpleName();
        if (!name.endsWith("ColumnCodec")) {
            throw new AssertionError();
        }
        name = name.substring(0, name.length() - 11);
        return name;
    }
}
