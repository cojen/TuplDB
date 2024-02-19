/*
 *  Copyright (C) 2023 Cojen.org
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.cojen.tupl.Table;

import org.cojen.tupl.table.join.JoinSpec;

/**
 * Defines a node which cross joins relations together into a new relation.
 *
 * @author Brian S. O'Neill
 */
public final class JoinNode extends RelationNode {
    /**
     * @param joinType see JoinSpec
     * @throws IllegalArgumentException if any duplicate join relation names
     */
    public static JoinNode make(int joinType, RelationNode left, RelationNode right) {
        boolean leftNullable = JoinSpec.isLeftNullable(joinType);
        boolean rightNullable = JoinSpec.isRightNullable(joinType);
        RelationType type = makeType(left, leftNullable, right, rightNullable);
        return new JoinNode(type, null, joinType, left, right);
    }

    private static RelationType makeType(RelationNode left, boolean leftNullable,
                                         RelationNode right, boolean rightNullable)
    {
        var columnMap = new LinkedHashMap<String, Column>();
        flattenColumns(left, leftNullable, columnMap);
        flattenColumns(right, rightNullable, columnMap);
        var columns = columnMap.values().toArray(new Column[columnMap.size()]);
        var cardinality = left.type().cardinality().multiply(right.type().cardinality());
        return RelationType.make(TupleType.make(columns), cardinality);
    }

    private static void flattenColumns(RelationNode node, boolean nullable,
                                       LinkedHashMap<String, Column> dst)
    {
        if (node instanceof JoinNode jn) {
            flattenColumns(jn.mLeft, nullable, dst);
            flattenColumns(jn.mRight, nullable, dst);
        } else {
            String name = node.name();
            TupleType type = node.type().tupleType();
            if (nullable) {
                type = type.nullable();
            }
            var column = Column.make(type, name, false);
            if (dst.putIfAbsent(name, column) != null) {
                throw new IllegalArgumentException("Duplicate join relation name: " + name);
            }
        }
    }

    private final int mJoinType;
    private final RelationNode mLeft, mRight;

    private TableProvider<?> mTableProvider;

    private JoinNode(RelationType type, String name,
                     int joinType, RelationNode left, RelationNode right)
    {
        super(type, name);
        mJoinType = joinType;
        mLeft = left;
        mRight = right;
    }

    @Override
    public JoinNode withName(String name) {
        return name.equals(name()) ? this : new JoinNode(type(), name, mJoinType, mLeft, mRight);
    }

    @Override
    public int maxArgument() {
        return Math.max(mLeft.maxArgument(), mRight.maxArgument());
    }

    @Override
    public boolean isPureFunction() {
        return mLeft.isPureFunction() && mRight.isPureFunction();
    }

    @Override
    public JoinNode replaceConstants(Map<ConstantNode, FieldNode> map, String prefix) {
        RelationNode left = mLeft.replaceConstants(map, prefix);
        RelationNode right = mRight.replaceConstants(map, prefix);
        if (left == mLeft && right == mRight) {
            return this;
        }
        return new JoinNode(type(), name(), mJoinType, left, right);
    }

    @Override
    public TableProvider<?> makeTableProvider() {
        if (mTableProvider == null) {
            mTableProvider = doMakeTableProvider();
        }
        return mTableProvider;
    }

    private TableProvider<?> doMakeTableProvider() {
        int argCount = maxArgument();

        var providerList = new ArrayList<TableProvider>();
        flattenProviders(this, providerList);

        TupleType tt = type().tupleType();
        Class<?> joinType = tt.clazz();
        String spec = makeSpec();

        Map<String, String> projectionMap = tt.makeProjectionMap();

        if (argCount == 0) {
            var tables = new Table[providerList.size()];
            for (int i=0; i<tables.length; i++) {
                tables[i] = providerList.get(i).table();
            }
            return TableProvider.make(Table.join(joinType, spec, tables), projectionMap);
        }

        var providers = providerList.toArray(new TableProvider[providerList.size()]);

        return new TableProvider() {
            @Override
            public Class rowType() {
                return joinType;
            }

            @Override
            public Map<String, String> projection() {
                return projectionMap;
            }

            @Override
            public int argumentCount() {
                return argCount;
            }

            @Override
            public Table table(Object... args) {
                var tables = new Table[providers.length];
                for (int i=0; i<tables.length; i++) {
                    tables[i] = providers[i].table(args);
                }
                return Table.join(joinType, spec, tables);
            }
        };
    }

    private static void flattenProviders(RelationNode node, ArrayList<TableProvider> dst) {
        if (node instanceof JoinNode jn) {
            flattenProviders(jn.mLeft, dst);
            flattenProviders(jn.mRight, dst);
        } else {
            dst.add(node.makeTableProvider());
        }
    }

    private String makeSpec() {
        var bob = new StringBuilder();
        TupleType tt = type().tupleType();
        int ci = appendSpec(tt, 0, bob);
        if (ci != tt.numColumns()) {
            throw new AssertionError();
        }
        return bob.toString();
    }

    /**
     * @param ci column index
     * @return updated column index
     */
    private int appendSpec(TupleType tt, int ci, StringBuilder bob) {
        ci = appendSpec(mLeft, tt, ci, bob);
        bob.append(' ').append(JoinSpec.typeToString(mJoinType)).append(' ');
        RelationNode right = mRight;
        if (right instanceof JoinNode rjn) {
            bob.append('(');
            ci = rjn.appendSpec(tt, ci, bob);
            bob.append(')');
        } else {
            ci = appendSpec(right, tt, ci, bob);
        }
        return ci;
    }

    /**
     * @param ci column index
     * @return updated column index
     */
    private static int appendSpec(RelationNode node, TupleType tt, int ci, StringBuilder bob) {
        if (node instanceof JoinNode jn) {
            ci = jn.appendSpec(tt, ci, bob);
        } else {
            bob.append(tt.field(ci++));
        }
        return ci;
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            assert mJoinType < 256;
            enc.encodeByte(mJoinType);
            mLeft.encodeKey(enc);
            mRight.encodeKey(enc);
        }
    }

    @Override
    public int hashCode() {
        int hash = mJoinType;
        hash = hash * 31 + mLeft.hashCode();
        hash = hash * 31 + mRight.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof JoinNode jn && mJoinType == jn.mJoinType
            && mLeft.equals(jn.mLeft) && mRight.equals(jn.mRight);
    }
}
