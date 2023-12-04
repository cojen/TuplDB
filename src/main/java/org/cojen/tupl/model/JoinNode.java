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

import org.cojen.tupl.jdbc.TableProvider;

import org.cojen.tupl.rows.join.JoinSpec;

/**
 * Defines a node which cross joins relations together into a new relation.
 *
 * @author Brian S. O'Neill
 */
public final class JoinNode extends RelationNode {
    /**
     * @param name can be null to automatically assign a name
     * @param joinType see JoinSpec
     * @throws IllegalArgumentException if any duplicate join relation names
     */
    public static JoinNode make(String name, int joinType, RelationNode left, RelationNode right) {
        return new JoinNode(makeType(left, right), name, joinType, left, right);
    }

    private static RelationType makeType(RelationNode left, RelationNode right) {
        var columnMap = new LinkedHashMap<String, Column>();
        flattenColumns(left, columnMap);
        flattenColumns(right, columnMap);
        var columns = columnMap.values().toArray(new Column[columnMap.size()]);
        var cardinality = left.type().cardinality().multiply(right.type().cardinality());
        return RelationType.make(TupleType.make(columns), cardinality);
    }

    private static void flattenColumns(RelationNode node, LinkedHashMap<String, Column> dst) {
        if (node instanceof JoinNode jn) {
            flattenColumns(jn.mLeft, dst);
            flattenColumns(jn.mRight, dst);
        } else {
            String name = node.name();
            var column = Column.make(node.type().tupleType(), name, false);
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
    public int maxArgument() {
        return Math.max(mLeft.maxArgument(), mRight.maxArgument());
    }

    @Override
    public boolean isPureFunction() {
        return mLeft.isPureFunction() && mRight.isPureFunction();
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

        Class<?> joinType = type().tupleType().clazz();
        String spec = makeSpec();

        Map<String, String> projectionMap = makeProjectionMap();

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
        int ci = appendSpec(0, bob);
        if (ci != type().tupleType().numColumns()) {
            throw new AssertionError();
        }
        return bob.toString();
    }

    /**
     * @param ci column index
     * @return updated column index
     */
    private int appendSpec(int ci, StringBuilder bob) {
        ci = appendSpec(mLeft, ci, bob);
        bob.append(' ').append(JoinSpec.typeToString(mJoinType)).append(' ');
        RelationNode right = mRight;
        if (right instanceof JoinNode rjn) {
            bob.append('(');
            ci = rjn.appendSpec(ci, bob);
            bob.append(')');
        } else {
            ci = appendSpec(right, ci, bob);
        }
        return ci;
    }

    /**
     * @param ci column index
     * @return updated column index
     */
    private int appendSpec(RelationNode node, int ci, StringBuilder bob) {
        if (node instanceof JoinNode jn) {
            ci = jn.appendSpec(ci, bob);
        } else {
            bob.append(type().tupleType().field(ci++));
        }
        return ci;
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
        return obj instanceof JoinNode jn && mJoinType == jn.mJoinType && mLeft.equals(mRight);
    }
}
