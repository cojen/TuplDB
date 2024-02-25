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

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.Converter;
import org.cojen.tupl.table.RowGen;
import org.cojen.tupl.table.RowInfo;

import org.cojen.tupl.views.ViewUtils;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class UpdateNode extends CommandNode {
    /**
     * @param table destination for the inserted rows
     * @param where optional
     * @param columns required table columns to set
     * @param values length must match columns
     */
    public static UpdateNode make(TableNode table, Node where,
                                  ColumnNode[] columns, Node[] values)
    {
        Objects.requireNonNull(table);
        if (columns.length != values.length) {
            throw new IllegalArgumentException();
        }

        if (where != null) {
            where = where.asType(BasicType.BOOLEAN);
            if (where instanceof ConstantNode cn && cn.value() == Boolean.TRUE) {
                where = null;
            }
        }

        return new UpdateNode("update", table, where, columns, values);
    }

    private final TableNode mTableNode;
    private Node mWhere;
    private final ColumnNode[] mColumns;
    private Node[] mValues;

    // These fields are set only if a simple update can be performed.
    private Map<ColumnNode, Node> mPkMatches;
    private Map<ConstantNode, FieldNode> mFieldMap;

    // These fields are only set if a full update is performed.
    private RelationNode mRelation;
    private String[] mRelationColumns; // fields corresponding to the columns to set
    private String[] mRelationValues;  // fields corresponding to the update values

    /**
     * @param relation can pass null to create when needed
     */
    private UpdateNode(String name, TableNode table, Node where,
                       ColumnNode[] columns, Node[] values)
    {
        super(name);

        mTableNode = table;
        mWhere = where;
        mColumns = columns;
        mValues = values;

        analyze();
    }

    /**
     * Copy and modify constructor.
     */
    private UpdateNode(UpdateNode un, String name) {
        super(name);

        // Changing the name doesn't change any other fields.
        mTableNode = un.mTableNode;
        mWhere = un.mWhere;
        mColumns = un.mColumns;
        mValues = un.mValues;
        mFieldMap = un.mFieldMap;
        mPkMatches = un.mPkMatches;
        mRelation = un.mRelation;
        mRelationColumns = un.mRelationColumns;
        mRelationValues = un.mRelationValues;
    }

    /**
     * Copy and modify constructor.
     */
    private UpdateNode(UpdateNode un, TableNode table, Node where,
                       ColumnNode[] columns, Node[] values)
    {
        super(un.name());

        mTableNode = table;
        mWhere = where;
        mColumns = columns;
        mValues = values;

        analyze();
    }

    /**
     * Fills in and modifies additional fields. Must be only be called by the constructor.
     */
    private void analyze() {
        // Check if the values are only params or constants.
        boolean basicValues = true;
        for (Node n : mValues) {
            if (!(n instanceof ParamNode || n instanceof ConstantNode)) {
                basicValues = false;
                break;
            }
        }

        // Check if a simple single row update against a primary key can be performed.
        if (basicValues && mWhere != null) simple: {
            var matches = new LinkedHashMap<ColumnNode, Node>();

            if (!mWhere.isSimpleDisjunction(matches)) {
                break simple;
            }

            RowInfo info = RowInfo.find(mTableNode.table().rowType());
            Map<String, ColumnInfo> pk = info.keyColumns;

            if (matches.size() != pk.size()) {
                break simple;
            }

            for (ColumnNode cn : matches.keySet()) {
                if (!pk.containsKey(cn.name())) {
                    break simple;
                }
            }

            // Can only update non-primary key columns.
            for (ColumnNode cn : mColumns) {
                if (pk.containsKey(cn.name())) {
                    break simple;
                }
            }

            mPkMatches = matches;

            mFieldMap = new LinkedHashMap<ConstantNode, FieldNode>();
            mWhere = mWhere.replaceConstants(mFieldMap, "$");
            mValues = Node.replaceConstants(mValues, mFieldMap, "$");

            return;
        }

        // Need to project all the columns and all the values. This ensures that rows of the
        // update relation contain all the columns to set and the values to set them to. To be
        // updatable, the relation must also refer to all of the primary key columns.

        var projectionMap = new LinkedHashMap<Node, Integer>();
        addToMap(projectionMap, mColumns);
        addToMap(projectionMap, mValues);

        {
            TupleType tt = mTableNode.type().tupleType();
            int num = tt.numColumns();
            for (int i=0; i<num; i++) {
                Column c = tt.column(i);
                ColumnNode cn = ColumnNode.make(mTableNode, c.name(), c);
                projectionMap.putIfAbsent(cn, projectionMap.size());
            }
        }

        Node[] projection = projectionMap.keySet().toArray(new Node[projectionMap.size()]);

        mRelation = SelectNode.make(mTableNode, mWhere, projection);
        TupleType tt = mRelation.type().tupleType();

        mRelationColumns = fieldNames(projectionMap, mColumns, tt);
        mRelationValues = fieldNames(projectionMap, mValues, tt);
    }

    private static void addToMap(Map<Node, Integer> projection, Node[] nodes) {
        for (Node n : nodes) {
            projection.putIfAbsent(n, projection.size());
        }
    }

    private static String[] fieldNames(Map<Node, Integer> projection, Node[] nodes, TupleType tt) {
        var names = new String[nodes.length];
        for (int i=0; i<names.length; i++) {
            names[i] = tt.field(projection.get(nodes[i]));
        }
        return names;
    }

    @Override
    public UpdateNode withName(String name) {
        if (name.equals(name())) {
            return this;
        }
        return new UpdateNode(this, name);
    }

    @Override
    public int maxArgument() {
        int max = mWhere == null ? 0 : mWhere.maxArgument();
        for (Node n : mValues) {
            max = Math.max(max, n.maxArgument());
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
    public UpdateNode replaceConstants(Map<ConstantNode, FieldNode> map, String prefix) {
        var table = mTableNode.replaceConstants(map, prefix);
        var where = mWhere;
        if (where != null) {
            where = where.replaceConstants(map, prefix);
        }
        var columns = (ColumnNode[]) Node.replaceConstants(mColumns, map, prefix);
        var values = (Node[]) Node.replaceConstants(mValues, map, prefix);

        if (table == mTableNode && where == mWhere && columns == mColumns && values == mValues) {
            return this;
        }

        return new UpdateNode(this, table, where, columns, values);
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected Object makeCode() {
        return makeCommandHandle();
    }

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            mTableNode.encodeKey(enc);
            enc.encodeOptionalNode(mWhere);
            enc.encodeNodes(mColumns);
            enc.encodeNodes(mValues);
        }
    }

    @Override
    public int hashCode() {
        int hash = mTableNode.hashCode();
        hash = hash * 31 + Objects.hashCode(mWhere);
        hash = hash * 31 + Arrays.hashCode(mColumns);
        hash = hash * 31 + Arrays.hashCode(mValues);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof UpdateNode un
            && mTableNode.equals(un.mTableNode)
            && Objects.equals(mWhere, un.mWhere)
            && Arrays.equals(mColumns, un.mColumns)
            && Arrays.equals(mValues, un.mValues);
    }

    @Override
    public Command makeCommand() {
        var mh = (MethodHandle) code(); // indirectly calls makeCommandHandle

        try {
            if (mRelation == null) {
                // simple update

                Object[] fieldValues;
                if (mFieldMap == null || mFieldMap.isEmpty()) {
                    fieldValues = null;
                } else {
                    fieldValues = new Object[mFieldMap.size()];
                    int ix = 0;
                    for (ConstantNode cn : mFieldMap.keySet()) {
                        fieldValues[ix++] = cn.canonicalValue();
                    }
                }

                return (Command) mh.invoke(mTableNode.table(), fieldValues);
            } else {
                // full update
                return (Command) mh.invoke(mRelation.makeTableProvider());
            }
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * Returns a MethodHandle to the Command constructor:
     *
     *     // for simple update
     *     (Table table, Object[] fieldValues);
     *
     *     // for full update
     *     (TableProvider provider);
     */
    private MethodHandle makeCommandHandle() {
        Class<?> rowType = mTableNode.table().rowType();

        ClassMaker cm = RowGen.beginClassMaker
            (UpdateNode.class, rowType, rowType.getName(), null, "update")
            .implement(Command.class).final_();

        // Keep a reference to the MethodHandle instance, to prevent it from being garbage
        // collected as long as the generated class still exists.
        cm.addField(Object.class, "_").static_().private_();

        MethodType mt;
        if (mRelation == null) {
            mt = makeSimpleUpdate(cm, rowType);
        } else {
            mt = makeFullUpdate(cm);
        }

        MethodHandles.Lookup lookup = cm.finishHidden();
        Class<?> clazz = lookup.lookupClass();

        try {
            MethodHandle mh = lookup.findConstructor(clazz, mt);

            // Assign the singleton reference.
            lookup.findStaticVarHandle(clazz, "_", Object.class).set(mh);

            return mh;
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    private MethodType makeFullUpdate(ClassMaker cm) {
        cm.addField(TableProvider.class, "provider").private_().final_();

        MethodType mt = MethodType.methodType(void.class, TableProvider.class);

        {
            MethodMaker ctor = cm.addConstructor(mt);
            ctor.invokeSuperConstructor();
            ctor.field("provider").set(ctor.param(0));
        }

        MethodMaker mm = cm.addMethod(int.class, "argumentCount").public_();
        mm.return_(maxArgument());

        mm = cm.addMethod(long.class, "exec", Transaction.class, Object[].class).public_();

        var txnVar = mm.param(0);
        var argsVar = mm.param(1);

        var tableVar = mm.field("provider").invoke("table", argsVar);

        var rowCountVar = mm.var(long.class).set(0);

        txnVar.set(mm.var(ViewUtils.class).invoke("enterScope", tableVar, txnVar));
        Label scopeStart = mm.label().here();

        // The query projection only needs the values. There's no point in loading the columns
        // to set if they're going to be modified.

        var b = new StringBuilder().append('{');
        for (int i=0; i<mRelationValues.length; i++) {
            if (i != 0) {
                b.append(", ");
            }
            b.append(mRelationValues[i]);
        }

        String projection = b.append('}').toString();

        var updaterVar = tableVar.invoke("newUpdater", txnVar, projection);

        Label tryStart = mm.label().here();

        // for (var updateRow = updater.row(); updateRow != null; ) {
        TupleType updateRowType = mRelation.type().tupleType();
        var updateRowVar = updaterVar.invoke("row").cast(updateRowType.clazz());
        Label loopStart = mm.label().here();
        Label loopEnd = mm.label();
        updateRowVar.ifEq(null, loopEnd);

        for (int i=0; i<mColumns.length; i++) {
            ColumnNode cn = mColumns[i];
            var srcVar = updateRowVar.invoke(mRelationValues[i]);
            var dstVar = mm.var(cn.type().clazz());
            Converter.convertExact(mm, cn.name(), mValues[i].type(), srcVar, cn.type(), dstVar);
            updateRowVar.invoke(mRelationColumns[i], dstVar);
        }

        updateRowVar.set(updaterVar.invoke("update", updateRowVar).cast(updateRowType.clazz()));
        rowCountVar.inc(1);

        // for loop end
        loopStart.goto_();
        loopEnd.here();

        // Updaters close automatically when they reach the end or when they throw an
        // exception. For this reason, a "catch" clause can be used instead of "finally".
        mm.catch_(tryStart, Throwable.class, (exVar) -> {
            updaterVar.invoke("close");
            exVar.throw_();
        });

        txnVar.invoke("commit");

        mm.finally_(scopeStart, () -> txnVar.invoke("exit"));
        mm.return_(rowCountVar);

        {
            mm = cm.addMethod(QueryPlan.class, "plan", Transaction.class, Object[].class).public_();
            mm.return_(mm.field("provider").invoke("table", mm.param(1)).invoke("queryAll")
                       .invoke("updaterPlan", mm.param(0)));
        }

        return mt;
    }

    private MethodType makeSimpleUpdate(ClassMaker cm, Class<?> rowType) {
        cm.addField(Table.class, "table").private_().final_();

        if (mFieldMap != null) {
            for (FieldNode fn : mFieldMap.values()) {
                cm.addField(fn.type().clazz(), fn.name()).private_().final_();
            }
        }

        MethodType mt = MethodType.methodType(void.class, Table.class, Object[].class);

        {
            MethodMaker ctor = cm.addConstructor(mt);
            ctor.invokeSuperConstructor();
            ctor.field("table").set(ctor.param(0));

            var fieldValuesVar = ctor.param(1);

            int ix = 0;
            for (FieldNode fn : mFieldMap.values()) {
                var fieldVar = fieldValuesVar.aget(ix++).cast(fn.type().clazz());
                ctor.field(fn.name()).set(fieldVar);
            }            
        }

        MethodMaker mm = cm.addMethod(int.class, "argumentCount").public_();
        mm.return_(maxArgument());

        mm = cm.addMethod(long.class, "exec", Transaction.class, Object[].class).public_();

        var txnVar = mm.param(0);
        var argsVar = mm.param(1);

        var tableVar = mm.field("table").get();
        var rowVar = tableVar.invoke("newRow").cast(rowType);
        var context = new EvalContext(argsVar, rowVar);

        for (Map.Entry<ColumnNode, Node> e : mPkMatches.entrySet()) {
            ColumnNode cn = e.getKey();
            Node value = e.getValue().asType(cn.type());
            var colVar = value.makeEval(context);
            rowVar.invoke(cn.column().name(), colVar);
        }

        for (int i=0; i<mColumns.length; i++) {
            ColumnNode cn = mColumns[i];
            Node value = mValues[i].asType(cn.type());
            var colVar = value.makeEval(context);
            rowVar.invoke(cn.column().name(), colVar);
        }

        var resultVar = tableVar.invoke("tryUpdate", txnVar, rowVar);

        var rowCountVar = mm.var(long.class);
        resultVar.ifTrue(() -> rowCountVar.set(1), () -> rowCountVar.set(0));

        mm.return_(rowCountVar);

        return mt;
    }
}
