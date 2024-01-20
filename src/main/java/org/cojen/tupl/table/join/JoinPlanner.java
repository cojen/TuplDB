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

package org.cojen.tupl.table.join;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.cojen.tupl.table.ColumnInfo;

import org.cojen.tupl.table.filter.ColumnFilter;
import org.cojen.tupl.table.filter.ColumnToArgFilter;
import org.cojen.tupl.table.filter.ColumnToColumnFilter;
import org.cojen.tupl.table.filter.ComplexFilterException;
import org.cojen.tupl.table.filter.RowFilter;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class JoinPlanner implements JoinSpec.Visitor {
    private final JoinSpec mPlannedSpec;
    private final int mOriginalNumArgs;
    private final int mTotalNumArgs;

    /**
     * @param spec must not contain any right joins
     * @param filter overall query filter against a join row
     */
    JoinPlanner(JoinSpec spec, RowFilter filter) {
        mOriginalNumArgs = filter.maxArgument();

        // First convert all Column instances to PlannedColumn instances, and make copies of
        // anything else that will be modified.
        JoinSpec.Node root = spec.root().accept(this);

        var split = new RowFilter[2];
        var distributer = new Distributer(split, filter);
        root = root.accept(distributer);

        var propagator = new ArgPropagator(split);
        root = root.accept(propagator);

        mPlannedSpec = new JoinSpec(root, distributer.mRemainder);
        mTotalNumArgs = mOriginalNumArgs + propagator.mArgMap.size();
    }

    /**
     * Returns a JoinSpec which consists of PlannedColumns.
     */
    public JoinSpec spec() {
        return mPlannedSpec;
    }

    /**
     * Returns the number of arguments which must be provided by the user.
     */
    public int originalNumArgs() {
        return mOriginalNumArgs;
    }

    /**
     * Returns the total number of arguments which are needed. The new arguments must be filled
     * in by inner loops.
     */
    public int totalNumArgs() {
        return mTotalNumArgs;
    }

    // Only required by the constructor.
    @Override
    public JoinSpec.Node visit(JoinSpec.Column node) {
        // A new instance is always needed because it will be modified.
        return new JoinSpec.PlannedColumn(node);
    }

    // Only required by the constructor.
    @Override
    public JoinSpec.Node visit(JoinSpec.FullJoin node) {
        // A new instance is always needed because it will be modified.
        return node.copyWith(this);
    }

    /**
     * Distributes a filter across join levels.
     */
    private static class Distributer extends FilterScorer implements JoinSpec.Visitor {
        final RowFilter[] mSplit;
        final Set<String> mAvailableNames = new HashSet<>();

        private ArrayList<String> mAvailableUndo;

        RowFilter mRemainder;

        Distributer(RowFilter[] split, RowFilter filter) {
            mSplit = split;
            try {
                mRemainder = filter.cnf();
            } catch (ComplexFilterException e) {
                // More terms will end up in the last remainder, which isn't optimal.
                mRemainder = filter;
            }
        }

        @Override
        public JoinSpec.Node visit(JoinSpec.Column node) {
            var planned = (JoinSpec.PlannedColumn) node;
            String joinName = planned.column().name;
            addAvailable(joinName);

            // The split checks prefixes and performs no modifications. The extracted filter is
            // the planned predicate, and the remainder will propagate to the next node.

            mRemainder.split((ColumnFilter filter) -> {
                ColumnInfo column1 = filter.column();
                if (!isAvailable(column1)) {
                    return null;
                }
                boolean match1 = joinName.equals(column1.prefix());
                if (filter instanceof ColumnToArgFilter) {
                    return match1 ? filter : null;
                }
                ColumnInfo column2 = ((ColumnToColumnFilter) filter).otherColumn();
                if (!isAvailable(column2)) {
                    return null;
                }
                if (match1) {
                    return filter;
                }
                return joinName.equals(column2.prefix()) ? filter : null;
            }, mSplit);

            // Initially the filter and predicate are the same, but the filter might be changed
            // later by ArgPropagator.
            planned.filter(mSplit[0]);
            planned.predicate(mSplit[0]);

            mRemainder = mSplit[1].reduceMore();

            return planned;
        }

        @Override
        public JoinSpec.Node visit(JoinSpec.FullJoin node) {
            // Similar to visit(JoinSpec.Column), except it acts on multiple columns.

            Map<String, JoinSpec.Column> columnMap = node.columnMap();

            for (String name : columnMap.keySet()) {
                addAvailable(name);
            }

            mRemainder.split((ColumnFilter filter) -> {
                ColumnInfo column1 = filter.column();
                if (!isAvailable(column1)) {
                    return null;
                }
                boolean match1 = columnMap.containsKey(column1.prefix());
                if (filter instanceof ColumnToArgFilter) {
                    return match1 ? filter : null;
                }
                ColumnInfo column2 = ((ColumnToColumnFilter) filter).otherColumn();
                if (!isAvailable(column2)) {
                    return null;
                }
                if (match1) {
                    return filter;
                }
                return columnMap.containsKey(column2.prefix()) ? filter : null;
            }, mSplit);

            // Initially the filter and predicate are the same, but the filter might be changed
            // later by ArgPropagator.
            node.filter(mSplit[0]);
            node.predicate(mSplit[0]);

            mRemainder = mSplit[1].reduceMore();

            return node;
        }

        @Override
        public JoinSpec.Node visit(JoinSpec.InnerJoins node) {
            final int savepoint = savepoint();
            final RowFilter remainder = mRemainder;

            JoinSpec.Node[] nodes = node.copyNodes();
            JoinSpec.Node[] bestNodes = new JoinSpec.Node[nodes.length];

            // FIXME: Configurable limit; presort in that case.
            permute(savepoint, remainder, bestNodes, nodes, nodes.length, nodes.length);

            // Need to reconstruct the available names after accepting the best sequence.
            rollback(savepoint);
            mRemainder = remainder;
            for (int i=0; i<bestNodes.length; i++) {
                bestNodes[i] = bestNodes[i].accept(this);
            }

            return new JoinSpec.InnerJoins(bestNodes, bestNodes.length);
        }

        /**
         * @param savepoint rollback is called for each permutation
         * @param remainder remainder to start with each time
         * @param bestNodes best nodes so far (is updated as a side effect)
         * @param nodes nodes to permute (is updated as a side effect)
         * @param k initially set to the end
         * @param end exclusive node range end
         */
        private void permute(int savepoint, RowFilter remainder,
                             JoinSpec.Node[] bestNodes, JoinSpec.Node[] nodes, int k, int end)
        {
            // Permute using Heap's algorithm.
            if (--k != 0) {
                permute(savepoint, remainder, bestNodes, nodes, k, end);
                if ((k & 1) == 0) {
                    for (int i=0; i<k; i++) {
                        swapNodes(nodes, 0, k);
                        permute(savepoint, remainder, bestNodes, nodes, k, end);
                    }
                } else {
                    for (int i=0; i<k; i++) {
                        swapNodes(nodes, i, k);
                        permute(savepoint, remainder, bestNodes, nodes, k, end);
                    }
                }
                return;
            }

            rollback(savepoint);
            mRemainder = remainder;

            Set<String> available = mAvailableNames;

            for (int i=0;;) {
                JoinSpec.Node node = nodes[i].accept(this);

                node.assignScore(this, available);

                JoinSpec.Node best = bestNodes[i];

                if (best == null) {
                    if (++i >= end) {
                        // Current permutation is better overall.
                        break;
                    }
                } else {
                    int cmp = node.compareScore(best);
                    if (++i >= end) {
                        if (cmp <= 0) {
                            // Current permutation is worse or the same.
                            return;
                        } else {
                            // Current permutation is better overall.
                            break;
                        }
                    } else {
                        if (cmp < 0) {
                            // Current permutation is worse.
                            return;
                        } else if (cmp > 0) {
                            // Current permutation is better overall.
                            do {
                                nodes[i].accept(this).assignScore(this, available);
                            } while (++i < end);
                            break;
                        }
                    }
                }
            }

            // Update the best, copying them to preserve the scores.

            for (int i=0; i<end; i++) {
                bestNodes[i] = nodes[i].accept(new JoinSpec.Visitor() {
                    @Override
                    public JoinSpec.Node visit(JoinSpec.Column node) {
                        return node.copyWithScore();
                    }

                    @Override
                    public JoinSpec.Node visit(JoinSpec.FullJoin node) {
                        return node.copyWithScore();
                    }
                });
            }
        }

        private static void swapNodes(JoinSpec.Node[] nodes, int a, int b) {
            JoinSpec.Node n = nodes[a];
            nodes[a] = nodes[b];
            nodes[b] = n;
        }

        /**
         * Returns true if the given column (expected to be a path) is available from an
         * earlier join level.
         */
        private boolean isAvailable(ColumnInfo column) {
            String name = column.name;
            if (mAvailableNames.contains(name)) {
                return true;
            }
            String prefix = column.prefix();
            if (prefix == null) {
                return false;
            }
            if (!mAvailableNames.contains(prefix)) {
                return false;
            }
            addAvailable(name);
            return true;
        }

        private void addAvailable(String name) {
            mAvailableNames.add(name);
            ArrayList<String> undo = mAvailableUndo;
            if (undo != null) {
                undo.add(name);
            }
        }

        /**
         * Create a savepoint for rolling back available names.
         */
        private int savepoint() {
            if (mAvailableUndo == null) {
                mAvailableUndo = new ArrayList<>();
                return 0;
            } else {
                return mAvailableUndo.size();
            }
        }

        /**
         * Rollback by removing available names which were added since the savepoint.
         */
        private void rollback(int savepoint) {
            Set<String> available = mAvailableNames;
            ArrayList<String> undo = mAvailableUndo;
            int size = undo.size();
            if (size == available.size()) {
                available.clear();
                undo.clear();
            } else while (--size >= savepoint) {
                available.remove(undo.remove(size));
            }
        }
    }

    /**
     * Prunes column prefixes and converts column-to-column filters into column-to-argument
     * filters where necessary.
     */
    private class ArgPropagator implements JoinSpec.Visitor {
        final RowFilter[] mSplit;
        final Map<String, JoinSpec.Source> mArgSources = new HashMap<>();
        final Map<String, Integer> mArgMap = new HashMap<>();

        ArgPropagator(RowFilter[] split) {
            mSplit = split;
        }

        @Override
        public JoinSpec.Node visit(JoinSpec.Column node) {
            var planned = (JoinSpec.PlannedColumn) node;
            String joinName = planned.column().name;

            // The split converts columns to arguments, unless conversion isn't possible. The
            // extracted filter is the planned filter, and the remainder is the planned
            // remainder.

            planned.filter().split((ColumnFilter filter) -> {
                ColumnInfo column1 = filter.column();

                if (filter instanceof ColumnToArgFilter c2a) {
                    assert joinName.equals(column1.prefix());
                    return c2a.withColumn(column1.tail());
                }

                var c2c = (ColumnToColumnFilter) filter;

                // Identify the target column to be replaced with an argument. Argument
                // assignments follow strict conversion rules, and so if the columns being
                // compared aren't exactly the same type, the target column might need to
                // remain as a column. The filter can still be applied to the planned column
                // remainder, which helps the nested loops join by performing filtering as
                // early as possible.

                ColumnInfo column2 = c2c.otherColumn();
                Integer argNum;

                if (joinName.equals(column1.prefix())) {
                    // Attempt to replace the second column with an argument.
                    if (canConvert(c2c, column1) && (argNum = argNumFor(node, column2)) != null) {
                        if (column2.isNullable() && !column1.isNullable()) {
                            // A negative argument number signals that a null check is required.
                            argNum = -argNum;
                        }
                        return new ColumnToArgFilter(column1.tail(), c2c.operator(), argNum);
                    }
                } else {
                    // Attempt to replace the first column with an argument.
                    if (canConvert(c2c, column2) && (argNum = argNumFor(node, column1)) != null) {
                        if (column1.isNullable() && !column2.isNullable()) {
                            // A negative argument number signals that a null check is required.
                            argNum = -argNum;
                        }
                        assert joinName.equals(column2.prefix());
                        int op = ColumnFilter.reverseOperator(c2c.operator());
                        return new ColumnToArgFilter(column2.tail(), op, argNum);
                    }
                }

                return null;
            }, mSplit);

            planned.filter(mSplit[0]);
            planned.remainder(mSplit[1]);

            mArgSources.put(joinName, planned);

            return planned;
        }

        @Override
        public JoinSpec.Node visit(JoinSpec.FullJoin node) {
            // Similar to visit(JoinSpec.Column), except it acts on multiple columns, and
            // column prefixes aren't removed.

            Map<String, JoinSpec.Column> columnMap = node.columnMap();

            node.filter().split((ColumnFilter filter) -> {
                ColumnInfo column1 = filter.column();

                if (filter instanceof ColumnToArgFilter) {
                    return null;
                }

                var c2c = (ColumnToColumnFilter) filter;

                ColumnInfo column2 = c2c.otherColumn();
                Integer argNum;

                if (columnMap.containsKey(column1.prefix())) {
                    // Attempt to replace the second column with an argument.
                    if (canConvert(c2c, column1) && (argNum = argNumFor(node, column2)) != null) {
                        if (column2.isNullable() && !column1.isNullable()) {
                            // A negative argument number signals that a null check is required.
                            argNum = -argNum;
                        }
                        return new ColumnToArgFilter(column1, c2c.operator(), argNum);
                    }
                } else {
                    // Attempt to replace the first column with an argument.
                    if (canConvert(c2c, column2) && (argNum = argNumFor(node, column1)) != null) {
                        if (column1.isNullable() && !column2.isNullable()) {
                            // A negative argument number signals that a null check is required.
                            argNum = -argNum;
                        }
                        assert columnMap.containsKey(column2.prefix());
                        return new ColumnToArgFilter(column2, c2c.operator(), argNum);
                    }
                }

                return null;
            }, mSplit);

            node.filter(mSplit[0].and(mSplit[1]));

            for (String name : columnMap.keySet()) {
                mArgSources.put(name, node);
            }

            return node;
        }

        private Integer argNumFor(JoinSpec.Source node, ColumnInfo column) {
            String prefix = column.prefix();

            JoinSpec.Source source = mArgSources.get(prefix);
            if (source == null) {
                return null;
            }

            String name = column.name;
            Integer argNum = mArgMap.get(name);

            if (argNum == null) {
                argNum = mOriginalNumArgs + mArgMap.size() + 1;
                source.addArgAssignment(argNum, column);
                mArgMap.put(name, argNum);
            }

            node.addArgSource(prefix, source);

            return argNum;
        }

        private static boolean canConvert(ColumnToColumnFilter filter, ColumnInfo target) {
            ColumnInfo common = filter.common();
            return target.type.isAssignableFrom(common.type)
                || target.boxedType().isAssignableFrom(common.type);
        }
    }
}
