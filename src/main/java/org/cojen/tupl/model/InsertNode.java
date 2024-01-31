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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.table.Converter;
import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.RowGen;
import org.cojen.tupl.table.RowInfo;

import org.cojen.tupl.views.ViewUtils;

/**
 * Defines a node which represents an SQL insert statement.
 *
 * @author Brian S. O'Neill
 */
public final class InsertNode extends CommandNode {
    /**
     * @param table destination for the inserted rows
     * @param columns required table columns; missing columns will have a default value
     * inserted, if a default is defined
     * @param values one row of values to insert; optional, length must match columns
     * @param select source of multiple rows; optional, number of columns must match columns
     * length
     */
    public static InsertNode make(TableNode table,
                                  ColumnNode[] columns, Node[] values, RelationNode select)
    {
        Objects.requireNonNull(table);
        Objects.requireNonNull(columns);
        if (values != null && values.length != columns.length) {
            throw new IllegalArgumentException();
        }
        // FIXME: Might need to check projection instead. Or perhaps see why
        // OrderedRelationNode is adding columns that aren't part of the projection.
        if (select != null && select.type().tupleType().numColumns() != columns.length) {
            throw new IllegalArgumentException();
        }

        RowInfo rowInfo = RowInfo.find(table.table().rowType());
        for (ColumnNode cn : columns) {
            String name = cn.column().name();
            if (!rowInfo.allColumns.containsKey(name)) {
                throw new IllegalStateException("Column isn't found: " + name);
            }
        }

        if (values != null) {
            // Must convert the values to the expected type. Do this before converting any
            // constants to fields, in order to avoid runtime conversions.

            boolean cloned = false;

            for (int i=0; i<values.length; i++) {
                ColumnInfo info = rowInfo.allColumns.get(columns[i].column().name());

                Node value = values[i];
                Node replaced = value.asType(BasicType.make(info));
                if (replaced != value) {
                    if (!cloned) {
                        values = values.clone();
                        cloned = true;
                    }
                    values[i] = replaced;
                }
            }
        }

        var fieldMap = new LinkedHashMap<ConstantNode, FieldNode>();
        values = Node.replaceConstants(values, fieldMap, "$");

        return new InsertNode("insert", table, columns, values, select, fieldMap);
    }

    private final TableNode mTableNode;
    private final ColumnNode[] mColumns;
    private final Node[] mValues;
    private final RelationNode mSelect;
    private final Map<ConstantNode, FieldNode> mFieldMap;

    private InsertNode(String name, TableNode table, ColumnNode[] columns, Node[] values,
                       RelationNode select, Map<ConstantNode, FieldNode> fieldMap)
    {
        super(name);
        mTableNode = table;
        mColumns = columns;
        mValues = values;
        mSelect = select;
        mFieldMap = fieldMap;
    }

    @Override
    public InsertNode withName(String name) {
        if (name.equals(name())) {
            return this;
        }
        return new InsertNode(name, mTableNode, mColumns, mValues, mSelect, mFieldMap);
    }

    @Override
    public int maxArgument() {
        int max = 0;
        if (mValues != null) {
            for (Node n : mValues) {
                max = Math.max(max, n.maxArgument());
            }
        }
        if (mSelect != null) {
            max = Math.max(max, mSelect.maxArgument());
        }
        return max;
    }

    @Override
    public void evalColumns(Set<String> columns) {
        // FIXME: revise when makeEval is implemented
    }

    @Override
    public Variable makeEval(EvalContext context) {
        // FIXME: makeEval
        throw null;
    }

    @Override
    public InsertNode replaceConstants(Map<ConstantNode, FieldNode> map, String prefix) {
        var columns = (ColumnNode[]) Node.replaceConstants(mColumns, map, prefix);
        RelationNode select = mSelect.replaceConstants(map, prefix);
        if (columns == mColumns && select == mSelect) {
            return this;
        }
        return new InsertNode(name(), mTableNode, columns, mValues, select, mFieldMap);
    }

    @Override
    public int hashCode() {
        int hash = mTableNode.hashCode();
        hash = hash * 31 + Arrays.hashCode(mValues);
        if (mSelect != null) {
            hash = hash * 31 + mSelect.hashCode();
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof InsertNode in && mTableNode == in.mTableNode 
            && Arrays.equals(mValues, in.mValues) && Objects.equals(mSelect, in.mSelect);
    }

    @Override
    public Command makeCommand() {
        MethodHandle mh = makeCommandClass();

        Object[] fieldValues;
        if (mFieldMap.isEmpty()) {
            fieldValues = null;
        } else {
            fieldValues = new Object[mFieldMap.size()];
            int ix = 0;
            for (ConstantNode cn : mFieldMap.keySet()) {
                fieldValues[ix++] = cn.canonicalValue();
            }
        }

        TableProvider select = mSelect == null ? null : mSelect.makeTableProvider();

        try {
            return (Command) mh.invoke(mTableNode.table(), fieldValues, select);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * Returns a MethodHandle to the Command constructor with this signature pattern:
     *
     *     (Table<?> table, Object[] fieldValues, TableProvider select)
     *
     * If there are no fields to set, the fieldValues parameter is Object and is ignored.
     * Likewise, if there's no select, the select parameter is Object and ignored.
     */
    private MethodHandle makeCommandClass() {
        // FIXME: Cache by rowType, field param types, select row type, column names, and value
        // nodes.

        Class<?> rowType = mTableNode.table().rowType();

        ClassMaker cm = RowGen.beginClassMaker
            (InsertNode.class, rowType, rowType.getName(), null, "insert")
            .implement(Command.class).final_();

        cm.addField(Table.class, "table").private_().final_();

        if (mSelect != null) {
            cm.addField(TableProvider.class, "select").private_().final_();
        }

        final MethodType mt;
        final MethodMaker ctor;

        if (mFieldMap.isEmpty()) {
            if (mSelect == null) {
                mt = MethodType.methodType
                    (void.class, Table.class, Object.class, Object.class);
                ctor = cm.addConstructor(mt);
            } else {
                mt = MethodType.methodType
                    (void.class, Table.class, Object.class, TableProvider.class);
                ctor = cm.addConstructor(mt);
                ctor.field("select").set(ctor.param(2));
            }
        } else {
            for (FieldNode fn : mFieldMap.values()) {
                cm.addField(fn.type().clazz(), fn.name()).private_().final_();
            }

            if (mSelect == null) {
                mt = MethodType.methodType
                    (void.class, Table.class, Object[].class, Object.class);
                ctor = cm.addConstructor(mt);
            } else {
                mt = MethodType.methodType
                    (void.class, Table.class, Object[].class, TableProvider.class);
                ctor = cm.addConstructor(mt);
                ctor.field("select").set(ctor.param(2));
            }

            var fieldValuesVar = ctor.param(1);

            int ix = 0;
            for (FieldNode fn : mFieldMap.values()) {
                var fieldVar = fieldValuesVar.aget(ix++).cast(fn.type().clazz());
                ctor.field(fn.name()).set(fieldVar);
            }            
        }

        ctor.field("table").set(ctor.param(0));
        ctor.invokeSuperConstructor();

        MethodMaker mm = cm.addMethod(int.class, "argumentCount").public_();
        mm.return_(maxArgument());

        mm = cm.addMethod(long.class, "exec", Transaction.class, Object[].class).public_();

        var txnVar = mm.param(0);
        var argsVar = mm.param(1);

        var tableVar = mm.field("table").get();

        Label scopeStart = null;
        if (mSelect != null) {
            txnVar.set(mm.var(ViewUtils.class).invoke("enterScope", tableVar, txnVar));
            scopeStart = mm.label().here();
        }

        var rowCountVar = mm.var(long.class);

        if (mFieldMap.isEmpty()) {
            rowCountVar.set(0);
        } else {
            var rowVar = tableVar.invoke("newRow").cast(rowType);
            var context = new EvalContext(argsVar, rowVar);
            for (int i=0; i<mValues.length; i++) {
                var colVar = mValues[i].makeEval(context);
                rowVar.invoke(mColumns[i].column().name(), colVar);
            }
            tableVar.invoke("insert", txnVar, rowVar);
            rowCountVar.set(1);
        }

        if (mSelect != null) {
            var scannerVar = mm.field("select").invoke("table", argsVar)
                .invoke("newScanner", txnVar);

            Label tryStart = mm.label().here();

            // for (var selectRow = scanner.row(); selectRow != null;
            //      selectRow = scanner.step(selectRow)) ...
            TupleType selectRowType = mSelect.type().tupleType();
            // Explicitly install the class because it might come from a different ClassLoader.
            cm.installClass(selectRowType.clazz());
            var selectRowVar = scannerVar.invoke("row").cast(selectRowType.clazz());
            Label loopStart = mm.label().here();
            Label loopEnd = mm.label();
            selectRowVar.ifEq(null, loopEnd);

            var rowVar = tableVar.invoke("newRow").cast(rowType);

            for (int i=0; i<mColumns.length; i++) {
                ColumnNode cn = mColumns[i];
                Column dstColumn = cn.column();
                Type dstType = dstColumn.type();
                var dstVar = mm.var(dstType.clazz());

                Converter.convertExact(mm, cn.name(), selectRowType.column(i).type(),
                                       selectRowVar.invoke(selectRowType.field(i)),
                                       dstType, dstVar);

                rowVar.invoke(dstColumn.name(), dstVar);
            }

            tableVar.invoke("insert", txnVar, rowVar);
            rowCountVar.inc(1);

            // for loop end
            selectRowVar.set(scannerVar.invoke("step", selectRowVar).cast(selectRowVar));
            loopStart.goto_();
            loopEnd.here();

            // Scanners close automatically when they reach the end or when they throw an
            // exception. For this reason, a "catch" clause can be used instead of "finally".
            mm.catch_(tryStart, Throwable.class, (exVar) -> {
                scannerVar.invoke("close");
                exVar.throw_();
            });

            txnVar.invoke("commit");

            mm.finally_(scopeStart, () -> txnVar.invoke("exit"));
        }

        mm.return_(rowCountVar);

        if (mSelect != null) {
            mm = cm.addMethod(QueryPlan.class, "plan", Transaction.class, Object[].class).public_();
            mm.return_(mm.field("select").invoke("table", mm.param(1)).invoke("queryAll")
                       .invoke("scannerPlan", mm.param(0)));
        }

        MethodHandles.Lookup lookup = cm.finishHidden();
        Class<?> clazz = lookup.lookupClass();

        try {
            return lookup.findConstructor(clazz, mt);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }
}
