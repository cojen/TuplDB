/*
 *  Copyright (C) 2021-2022 Cojen.org
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

package org.cojen.tupl.rows;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.util.Map;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Field;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.diag.QueryPlan;

/**
 * Base class for TheTableMaker and JoinedTableMaker.
 *
 * @author Brian S O'Neill
 */
public class TableMaker {
    protected final Class<?> mRowType;
    protected final RowGen mRowGen;
    protected final RowInfo mRowInfo;
    protected final RowGen mCodecGen;
    protected final Class<?> mRowClass;
    protected final byte[] mSecondaryDescriptor;

    protected ClassMaker mClassMaker;

    /**
     * @param rowGen describes row encoding
     * @param codecGen describes key and value codecs (can be different than rowGen)
     * @param secondaryDesc secondary index descriptor
     */
    TableMaker(Class<?> type, RowGen rowGen, RowGen codecGen, byte[] secondaryDesc) {
        mRowType = type;
        mRowGen = rowGen;
        mRowInfo = rowGen.info;
        mCodecGen = codecGen;
        mRowClass = RowMaker.find(type);
        mSecondaryDescriptor = secondaryDesc;
    }

    protected MethodHandle doFinish(MethodType mt) {
        try {
            var lookup = mClassMaker.finishLookup();
            return lookup.findConstructor(lookup.lookupClass(), mt);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    protected boolean isPrimaryTable() {
        return mRowGen == mCodecGen;
    }

    /**
     * @return null if no field is defined for the column (probably SchemaVersionColumnCodec)
     */
    protected static Field findField(Variable row, ColumnCodec codec) {
        ColumnInfo info = codec.mInfo;
        return info == null ? null : row.field(info.name);
    }

    protected void markAllClean(Variable rowVar) {
        markAllClean(rowVar, mRowGen, mCodecGen);
    }

    protected static void markAllClean(Variable rowVar, RowGen rowGen, RowGen codecGen) {
        if (rowGen == codecGen) { // isPrimaryTable, so truly mark all clean
            int mask = 0x5555_5555;
            int i = 0;
            String[] stateFields = rowGen.stateFields();
            for (; i < stateFields.length - 1; i++) {
                rowVar.field(stateFields[i]).set(mask);
            }
            mask >>>= (32 - ((rowGen.info.allColumns.size() & 0b1111) << 1));
            rowVar.field(stateFields[i]).set(mask);
        } else {
            // Only mark columns clean that are defined by codecGen. All others are unset.
            markClean(rowVar, rowGen, codecGen.info.allColumns);
        }
    }

    /**
     * Mark only the given columns as CLEAN. All others are UNSET.
     */
    protected static void markClean(final Variable rowVar, final RowGen rowGen,
                                    final Map<String, ColumnInfo> columns)
    {
        final int maxNum = rowGen.info.allColumns.size();

        int num = 0, mask = 0;

        for (int step = 0; step < 2; step++) {
            // Key columns are numbered before value columns. Add checks in two steps.
            // Note that the codecs are accessed, to match encoding order.
            var baseCodecs = step == 0 ? rowGen.keyCodecs() : rowGen.valueCodecs();

            for (ColumnCodec codec : baseCodecs) {
                if (columns.containsKey(codec.mInfo.name)) {
                    mask |= RowGen.stateFieldMask(num, 0b01); // clean state
                }
                if ((++num & 0b1111) == 0 || num >= maxNum) {
                    rowVar.field(rowGen.stateField(num - 1)).set(mask);
                    mask = 0;
                }
            }
        }
    }

    /**
     * Remaining states are UNSET or CLEAN.
     */
    protected static void markAllUndirty(Variable rowVar, RowInfo info) {
        int mask = 0x5555_5555;
        int i = 0;
        String[] stateFields = info.rowGen().stateFields();
        for (; i < stateFields.length - 1; i++) {
            var field = rowVar.field(stateFields[i]);
            field.set(field.and(mask));
        }
        mask >>>= (32 - ((info.allColumns.size() & 0b1111) << 1));
        var field = rowVar.field(stateFields[i]);
        field.set(field.and(mask));
    }

    /**
     * Mark all the value columns as UNSET without modifying the key column states.
     */
    protected void markValuesUnset(Variable rowVar) {
        if (isPrimaryTable()) {
            // Clear the value column state fields. Skip the key columns, which are numbered
            // first. Note that the codecs are accessed, to match encoding order.
            int num = mRowInfo.keyColumns.size();
            int mask = 0;
            for (ColumnCodec codec : mRowGen.valueCodecs()) {
                mask |= RowGen.stateFieldMask(num);
                if (isMaskReady(++num, mask)) {
                    mask = maskRemainder(num, mask);
                    Field field = stateField(rowVar, num - 1);
                    mask = ~mask;
                    if (mask == 0) {
                        field.set(mask);
                    } else {
                        field.set(field.and(mask));
                        mask = 0;
                    }
                }
            }
            return;
        }

        final Map<String, ColumnInfo> keyColumns = mCodecGen.info.keyColumns;
        final int maxNum = mRowInfo.allColumns.size();

        int num = 0, mask = 0;

        for (int step = 0; step < 2; step++) {
            // Key columns are numbered before value columns. Add checks in two steps.
            // Note that the codecs are accessed, to match encoding order.
            var baseCodecs = step == 0 ? mRowGen.keyCodecs() : mRowGen.valueCodecs();

            for (ColumnCodec codec : baseCodecs) {
                if (!keyColumns.containsKey(codec.mInfo.name)) {
                    mask |= RowGen.stateFieldMask(num);
                }
                if ((++num & 0b1111) == 0 || num >= maxNum) {
                    Field field = rowVar.field(mRowGen.stateField(num - 1));
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
     * Called when building state field masks for columns, when iterating them in order.
     *
     * @param num column number pre-incremented to the next one
     * @param mask current group; must be non-zero to have any effect
     */
    protected boolean isMaskReady(int num, int mask) {
        return mask != 0 && ((num & 0b1111) == 0 || num >= mRowInfo.allColumns.size());
    }

    /**
     * When building a mask for the highest state field, sets the high unused bits on the
     * mask. This can eliminate an unnecessary 'and' operation.
     *
     * @param num column number pre-incremented to the next one
     * @param mask current group
     * @return updated mask
     */
    protected int maskRemainder(int num, int mask) {
        if (num >= mRowInfo.allColumns.size()) {
            int shift = (num & 0b1111) << 1;
            if (shift != 0) {
                mask |= 0xffff_ffff << shift;
            }
        }
        return mask;
    }

    protected Field stateField(Variable rowVar, int columnNum) {
        return rowVar.field(mRowGen.stateField(columnNum));
    }

    /**
     * @param option bit 1: reverse, bit 2: joined
     */
    protected void addPlanMethod(int option) {
        String name = "plan";
        if ((option & 0b01) != 0) {
            name += "Reverse";
        }
        MethodMaker mm = mClassMaker.addMethod
            (QueryPlan.class, name, Object[].class).varargs().public_();
        var condy = mm.var(TableMaker.class).condy
            ("condyPlan", mRowType, mSecondaryDescriptor, option);
        mm.return_(condy.invoke(QueryPlan.class, "plan"));
    }

    /**
     * @param option bit 1: reverse, bit 2: joined
     */
    public static QueryPlan condyPlan(MethodHandles.Lookup lookup, String name, Class type,
                                      Class rowType, byte[] secondaryDesc, int option)
    {
        RowInfo primaryRowInfo = RowInfo.find(rowType);

        RowInfo rowInfo;
        String which;

        if (secondaryDesc == null) {
            rowInfo = primaryRowInfo;
            which = "primary key";
        } else {
            rowInfo = RowStore.secondaryRowInfo(primaryRowInfo, secondaryDesc);
            which = rowInfo.isAltKey() ? "alternate key" : "secondary index";
        }

        boolean reverse = (option & 0b01) != 0;
        QueryPlan plan = new QueryPlan.FullScan(rowInfo.name, which, rowInfo.keySpec(), reverse);
    
        if ((option & 0b10) != 0) {
            rowInfo = primaryRowInfo;
            plan = new QueryPlan.NaturalJoin(rowInfo.name, "primary key", rowInfo.keySpec(), plan);
        }

        return plan;
    }
}
