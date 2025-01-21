/*
 *  Copyright (C) 2022 Cojen.org
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

package org.cojen.tupl.diag;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * A query plan tree structure.
 *
 * @author Brian S O'Neill
 */
public abstract sealed class QueryPlan implements Serializable {
    QueryPlan() {
    }

    @Override
    public final String toString() {
        var b = new StringBuilder();
        appendTo(b);
        return b.toString();
    }

    public final void printTo(PrintStream out) {
        try {
            appendTo((Appendable) out);
        } catch (IOException e) {
            // Not expected.
        }
    }

    public final void appendTo(StringBuilder b) {
        try {
            appendTo((Appendable) b);
        } catch (IOException e) {
            // Not expected.
        }
    }

    public final void appendTo(Appendable a) throws IOException {
        appendTo(a, "- ", "  ");
    }

    /**
     * @param in1 indent to use for first line
     * @param in2 indent to use for remaining lines
     */
    abstract void appendTo(Appendable a, String in1, String in2) throws IOException;

    private static Appendable appendItem(Appendable a, String indent, String title)
        throws IOException
    {
        return a.append(indent).append(title).append(": ");
    }

    private static Appendable appendArray(Appendable a, String[] array) throws IOException {
        if (array != null) {
            for (int i=0; i<array.length; i++) {
                if (i > 0) {
                    a.append(", ");
                }
                a.append(array[i]);
            }
        }
        return a;
    }

    /**
     * @param title optional
     */
    private static Appendable appendSub(Appendable a, String indent, String title, QueryPlan sub)
        throws IOException
    {
        if (sub != null) {
            String in1 = indent + "- ";
            if (title != null) {
                in1 = in1 + title + ": ";
            }
            String in2 = indent + "  ";
            sub.appendTo(a, in1, in2);
        }
        return a;
    }

    /**
     * Query plan node which accesses rows from a table.
     */
    public static abstract sealed class Table extends QueryPlan {
        public final String table;
        public final String which;
        public final String[] keyColumns;

        /**
         * @param which primary key, alternate key, or secondary index
         * @param keyColumns columns with '+' or '-' prefixes
         */
        Table(String table, String which, String[] keyColumns) {
            this.table = table;
            this.which = which;
            this.keyColumns = keyColumns;
        }

        Appendable appendKeyColumns(Appendable a, String indent) throws IOException {
            appendItem(a, indent, "key columns");
            return appendArray(a, keyColumns);
        }

        boolean matches(Table other) {
            return Objects.equals(table, other.table) && Objects.equals(which, other.which)
                && Arrays.equals(keyColumns, other.keyColumns);
        }

        @Override
        public int hashCode() {
            int hash = Objects.hashCode(table);
            hash = hash * 31 + Objects.hashCode(which);
            hash = hash * 31 + Arrays.hashCode(keyColumns);
            return hash;
        }
    }

    /**
     * Query plan node which scans a table.
     */
    public static abstract sealed class Scan extends Table {
        public final boolean reverse;

        /**
         * @param which primary key, alternate key, or secondary index
         * @param keyColumns columns with '+' or '-' prefix
         * @param reverse true if a reverse scan
         */
        Scan(String table, String which, String[] keyColumns, boolean reverse) {
            super(table, which, keyColumns);
            this.reverse = reverse;
        }

        void appendTo(Appendable a, String in1, String in2, String title) throws IOException {
            a.append(in1);
            if (reverse) {
                a.append("reverse ");
            }
            a.append(title).append(" scan over ").append(which).append(": ")
                .append(table).append('\n');
            appendKeyColumns(a, in2).append('\n');
        }

        boolean matches(Scan other) {
            return super.matches(other) && reverse == other.reverse;
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ (reverse ? 754613080 : 1644587376);
        }
    }

    /**
     * Query plan node which scans all rows of a table.
     */
    public static final class FullScan extends Scan {
        private static final long serialVersionUID = 1L;

        /**
         * @param which primary key, alternate key, or secondary index
         * @param keyColumns columns with '+' or '-' prefix
         * @param reverse true if a reverse scan
         */
        public FullScan(String table, String which, String[] keyColumns, boolean reverse) {
            super(table, which, keyColumns, reverse);
        }

        @Override
        void appendTo(Appendable a, String in1, String in2) throws IOException {
            appendTo(a, in1, in2, "full");
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof FullScan scan && matches(scan);
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ 1727515038;
        }
    }

    /**
     * Query plan node which scans a range of rows from a table.
     */
    public static final class RangeScan extends Scan {
        private static final long serialVersionUID = 1L;

        public final String low, high;

        /**
         * @param which primary key, alternate key, or secondary index
         * @param keyColumns columns with '+' or '-' prefix
         * @param reverse true if a reverse scan
         * @param low low bound filter expression (or null if open)
         * @param high high bound filter expression (or null if open)
         */
        public RangeScan(String table, String which, String[] keyColumns, boolean reverse,
                         String low, String high)
        {
            super(table, which, keyColumns, reverse);
            this.low = low;
            this.high = high;
        }

        @Override
        void appendTo(Appendable a, String in1, String in2) throws IOException {
            appendTo(a, in1, in2, "range");
            appendItem(a, in2, "range");

            if (low != null) {
                a.append(low).append(' ');
            }

            a.append("..");

            if (high != null) {
                a.append(' ').append(high);
            }

            a.append('\n');
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof RangeScan scan && matches(scan);
        }

        boolean matches(RangeScan other) {
            return super.matches(other)
                && Objects.equals(low, other.low) && Objects.equals(high, other.high);
        }

        @Override
        public int hashCode() {
            int hash = super.hashCode();
            hash = hash * 31 + Objects.hashCode(low);
            hash = hash * 31 + Objects.hashCode(high);
            return hash ^ 169742416;
        }
    }

    /**
     * Query plan node which loads at most one row from a table.
     */
    public static final class LoadOne extends Table {
        private static final long serialVersionUID = 1L;

        public final String filter;

        /**
         * @param which primary key, alternate key, or secondary index
         * @param keyColumns columns with '+' or '-' prefix
         * @param filter filter which matches to the row (filter can be null if unspecified)
         */
        public LoadOne(String table, String which, String[] keyColumns, String filter) {
            super(table, which, keyColumns);
            this.filter = filter;
        }

        @Override
        void appendTo(Appendable a, String in1, String in2) throws IOException {
            a.append(in1).append("load one using ").append(which).append(": ")
                .append(table).append('\n');
            appendKeyColumns(a, in2).append('\n');
            if (filter != null) {
                appendItem(a, in2, "filter").append(filter).append('\n');
            }
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof LoadOne load && matches(load);
        }

        boolean matches(LoadOne other) {
            return super.matches(other) && Objects.equals(filter, other.filter);
        }

        @Override
        public int hashCode() {
            int hash = super.hashCode();
            hash = hash * 31 + Objects.hashCode(filter);
            return hash ^ -1241565554;
        }
    }

    /**
     * Query plan node which represents a single row with no columns.
     */
    public static final class Identity extends QueryPlan {
        public Identity() {
        }

        @Override
        void appendTo(Appendable a, String in1, String in2) throws IOException {
            a.append(in1).append("identity").append('\n');
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Identity;
        }

        @Override
        public int hashCode() {
            return -630610878;
        }
    }

    /**
     * Query plan node which filters out rows.
     */
    public static final class Filter extends QueryPlan {
        private static final long serialVersionUID = 1L;

        public final String expression;
        public final QueryPlan source;

        /**
         * @param expression filter expression
         * @param source child plan node
         */
        public Filter(String expression, QueryPlan source) {
            this.expression = expression;
            this.source = source;
        }

        @Override
        void appendTo(Appendable a, String in1, String in2) throws IOException {
            a.append(in1).append("filter").append(": ").append(expression).append('\n');
            appendSub(a, in2, null, source);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Filter filter && matches(filter);
        }

        boolean matches(Filter other) {
            return Objects.equals(expression, other.expression)
                && Objects.equals(source, other.source);
        }

        @Override
        public int hashCode() {
            int hash = Objects.hashCode(expression);
            hash = hash * 31 + Objects.hashCode(source);
            return hash ^ -274869396;
        }
    }

    /**
     * Query plan node which applies custom row mapping and filtering.
     */
    public static final class Mapper extends QueryPlan {
        private static final long serialVersionUID = 1L;

        public final String target;
        public final String operation;
        public final QueryPlan source;

        /**
         * @param target describes the target row type
         * @param operation describes the map operation (optional)
         * @param source child plan node
         */
        public Mapper(String target, String operation, QueryPlan source) {
            this.target = target;
            this.operation = operation;
            this.source = source;
        }

        public Mapper withOperation(String operation) {
            return new Mapper(target, operation, source);
        }

        @Override
        void appendTo(Appendable a, String in1, String in2) throws IOException {
            a.append(in1).append("map").append(": ").append(target).append('\n');
            if (operation != null) {
                appendItem(a, in2, "operation").append(operation).append('\n');
            }
            appendSub(a, in2, null, source);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Mapper mapper && matches(mapper);
        }

        boolean matches(Mapper other) {
            return Objects.equals(target, other.target)
                && Objects.equals(operation, other.operation)
                && Objects.equals(source, other.source);
        }

        @Override
        public int hashCode() {
            int hash = Objects.hashCode(target);
            hash = hash * 31 + Objects.hashCode(operation);
            hash = hash * 31 + Objects.hashCode(source);
            return hash ^ -677855948;
        }
    }

    /**
     * Query plan node which applies aggregation.
     */
    public static final class Aggregator extends QueryPlan {
        private static final long serialVersionUID = 1L;

        public final String target;
        public final String operation;
        public final String[] groupBy;
        public final QueryPlan source;

        /**
         * @param target describes the target row type
         * @param operation describes the aggregate operation (optional)
         * @param groupBy group-by columns (or null if none)
         * @param source child plan node
         */
        public Aggregator(String target, String operation, String[] groupBy, QueryPlan source) {
            this.target = target;
            this.operation = operation;
            this.groupBy = groupBy;
            this.source = source;
        }

        public Aggregator withOperation(String operation) {
            return new Aggregator(target, operation, groupBy, source);
        }

        @Override
        void appendTo(Appendable a, String in1, String in2) throws IOException {
            a.append(in1).append("aggregate").append(": ").append(target).append('\n');
            if (operation != null) {
                appendItem(a, in2, "operation").append(operation).append('\n');
            }
            if (groupBy != null && groupBy.length != 0) {
                appendItem(a, in2, "group by");
                appendArray(a, groupBy).append('\n');
            }
            appendSub(a, in2, null, source);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Aggregator aggregator && matches(aggregator);
        }

        boolean matches(Aggregator other) {
            return Objects.equals(target, other.target)
                && Objects.equals(operation, other.operation)
                && Arrays.equals(groupBy, other.groupBy) && Objects.equals(source, other.source);
        }

        @Override
        public int hashCode() {
            int hash = Objects.hashCode(target);
            hash = hash * 31 + Objects.hashCode(operation);
            hash = hash * 31 + Arrays.hashCode(groupBy);
            hash = hash * 31 + Objects.hashCode(source);
            return hash ^ -356180746;
        }
    }

    /**
     * Query plan node which applies grouping.
     */
    public static final class Grouper extends QueryPlan {
        private static final long serialVersionUID = 1L;

        public final String target;
        public final String operation;
        public final String[] groupBy, orderBy;
        public final QueryPlan source;

        /**
         * @param target describes the target row type
         * @param operation describes the group operation (optional)
         * @param groupBy group-by columns (or null if none)
         * @param orderBy order-by columns (or null if none)
         * @param source child plan node
         */
        public Grouper(String target, String operation,
                       String[] groupBy, String[] orderBy,
                       QueryPlan source)
        {
            this.target = target;
            this.operation = operation;
            this.groupBy = groupBy;
            this.orderBy = orderBy;
            this.source = source;
        }

        public Grouper withOperation(String operation) {
            return new Grouper(target, operation, groupBy, orderBy, source);
        }

        @Override
        void appendTo(Appendable a, String in1, String in2) throws IOException {
            a.append(in1).append("group").append(": ").append(target).append('\n');
            if (operation != null) {
                appendItem(a, in2, "operation").append(operation).append('\n');
            }
            if (groupBy != null && groupBy.length != 0) {
                appendItem(a, in2, "group by");
                appendArray(a, groupBy).append('\n');
            }
            if (orderBy != null && orderBy.length != 0) {
                appendItem(a, in2, "order by");
                appendArray(a, orderBy).append('\n');
            }
            appendSub(a, in2, null, source);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Grouper grouper && matches(grouper);
        }

        boolean matches(Grouper other) {
            return Objects.equals(target, other.target)
                && Objects.equals(operation, other.operation)
                && Arrays.equals(groupBy, other.groupBy) && Arrays.equals(orderBy, other.orderBy)
                && Objects.equals(source, other.source);
        }

        @Override
        public int hashCode() {
            int hash = Objects.hashCode(target);
            hash = hash * 31 + Objects.hashCode(operation);
            hash = hash * 31 + Arrays.hashCode(groupBy);
            hash = hash * 31 + Arrays.hashCode(orderBy);
            hash = hash * 31 + Objects.hashCode(source);
            return hash ^ -1001713878;
        }
    }

    /**
     * Query plan node which only checks for the existance of at least one row.
     */
    public static final class Exists extends QueryPlan {
        private static final long serialVersionUID = 1L;

        public final QueryPlan source;

        /**
         * @param source child plan node
         */
        public Exists(QueryPlan source) {
            this.source = source;
        }

        @Override
        void appendTo(Appendable a, String in1, String in2) throws IOException {
            a.append(in1).append("exists").append('\n');
            appendSub(a, in2, null, source);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Exists exists && matches(exists);
        }

        boolean matches(Exists other) {
            return Objects.equals(source, other.source);
        }

        @Override
        public int hashCode() {
            int hash = Objects.hashCode(source);
            return hash ^ -343981398;
        }
    }

    /**
     * Query plan node which sorts the rows.
     */
    public static sealed class Sort extends QueryPlan {
        private static final long serialVersionUID = 1L;

        public final String[] sortColumns;
        public final QueryPlan source;

        /**
         * @param sortColumns columns with '+' or '-' prefix
         * @param source child plan node
         */
        public Sort(String[] sortColumns, QueryPlan source) {
            this.sortColumns = sortColumns;
            this.source = source;
        }

        @Override
        void appendTo(Appendable a, String in1, String in2) throws IOException {
            a.append(in1).append("sort").append(": ");
            appendArray(a, sortColumns).append('\n');
            appendSub(a, in2, null, source);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Sort sort && matches(sort);
        }

        boolean matches(Sort other) {
            return Arrays.equals(sortColumns, other.sortColumns)
                && Objects.equals(source, other.source);
        }

        @Override
        public int hashCode() {
            int hash = Arrays.hashCode(sortColumns);
            hash = hash * 31 + Objects.hashCode(source);
            return hash ^ -419131761;
        }
    }

    /**
     * Query plan node which sorts rows within a group. The groups are already ordered
     * correctly with respect to other groups, and so a full sort isn't required.
     */
    public static final class GroupSort extends Sort {
        private static final long serialVersionUID = 1L;

        public final String[] groupColumns;

        /**
         * @param groupColumns columns with '+' or '-' prefix
         * @param sortColumns columns with '+' or '-' prefix
         * @param source child plan node
         */
        public GroupSort(String[] groupColumns, String[] sortColumns, QueryPlan source) {
            super(sortColumns, source);
            this.groupColumns = groupColumns;
        }

        @Override
        void appendTo(Appendable a, String in1, String in2) throws IOException {
            a.append(in1).append("group sort").append(": [");
            appendArray(a, groupColumns).append("], ");
            appendArray(a, sortColumns).append('\n');
            appendSub(a, in2, null, source);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof GroupSort sort && matches(sort);
        }

        boolean matches(GroupSort other) {
            return super.equals(other) && Arrays.equals(groupColumns, other.groupColumns);
        }

        @Override
        public int hashCode() {
            int hash = super.hashCode();
            hash = hash * 31 + Arrays.hashCode(groupColumns);
            return hash ^ 1586560018;
        }
    }

    /**
     * Query plan node which joins a target to a source based on a common set of columns.
     */
    public static sealed class NaturalJoin extends QueryPlan {
        private static final long serialVersionUID = 1L;

        public final String[] columns;
        public final QueryPlan target;
        public final QueryPlan source;

        public NaturalJoin(String[] columns, QueryPlan target, QueryPlan source) {
            this.columns = columns;
            this.target = target;
            this.source = source;
        }

        @Override
        void appendTo(Appendable a, String in1, String in2) throws IOException {
            a.append(in1).append("natural join").append('\n');
            appendItem(a, in2, "columns");
            appendArray(a, columns).append('\n');
            appendSub(a, in2, "target", target);
            appendSub(a, in2, "source", source);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof NaturalJoin join && matches(join);
        }

        boolean matches(NaturalJoin other) {
            return Arrays.equals(columns, other.columns)
                && Objects.equals(target, other.target) && Objects.equals(source, other.source);
        }

        @Override
        public int hashCode() {
            int hash = Objects.hashCode(source);
            hash = hash * 31 + Objects.hashCode(target);
            hash = hash * 31 + Arrays.hashCode(columns);
            return hash ^ 2016719916;
        }
    }

    /**
     * Query plan node which joins index rows to primary rows.
     */
    public static final class PrimaryJoin extends NaturalJoin {
        private static final long serialVersionUID = 1L;

        public final String table;

        /**
         * @param keyColumns columns with '+' or '-' prefix
         * @param source child plan node
         */
        public PrimaryJoin(String table, String[] keyColumns, QueryPlan source) {
            super(keyColumns, new LoadOne(table, "primary key", keyColumns, null), source);
            this.table = table;
        }

        @Override
        void appendTo(Appendable a, String in1, String in2) throws IOException {
            a.append(in1).append("primary join").append(": ").append(table).append('\n');
            appendItem(a, in2, "key columns");
            appendArray(a, columns).append('\n');
            appendSub(a, in2, null, source);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof PrimaryJoin join && matches(join);
        }

        boolean matches(PrimaryJoin other) {
            return super.matches(other) && Objects.equals(table, other.table);
        }

        @Override
        public int hashCode() {
            int hash = super.hashCode();
            hash = hash * 31 + Objects.hashCode(table);
            return hash ^ 2047385165;
        }
    }

    /**
     * Query plan node which represents a set of plans.
     */
    public static abstract sealed class Set extends QueryPlan {
        public final QueryPlan[] sources;

        /**
         * @param sources child plan nodes
         */
        Set(QueryPlan... sources) {
            this.sources = sources;
        }

        void appendTo(Appendable a, String in1, String in2, String title) throws IOException {
            a.append(in1).append(title).append('\n');
            if (sources != null) {
                for (int i=0; i<sources.length; i++) {
                    appendSub(a, in2, null, sources[i]);
                }
            }
        }

        boolean matches(Set other) {
            return Arrays.equals(sources, other.sources);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(sources) ^ 849140774;
        }
    }

    /**
     * Query plan node which represents an empty set.
     */
    public static final class Empty extends Set {
        public Empty() {
        }

        @Override
        void appendTo(Appendable a, String in1, String in2) throws IOException {
            appendTo(a, in1, in2, "empty");
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Empty;
        }

        @Override
        public int hashCode() {
            return 791511942;
        }
    }

    /**
     * Query plan node which represents a union set of plans.
     */
    public static abstract sealed class Union extends Set {
        /**
         * @param sources child plan nodes
         */
        Union(QueryPlan... sources) {
            super(sources);
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ 1505076886;
        }
    }

    /**
     * Query plan node which represents a union set of plans, where each source plan only
     * produces rows which aren't produced by the other sources.
     */
    public static final class DisjointUnion extends Union {
        private static final long serialVersionUID = 1L;

        /**
         * @param sources child plan nodes
         */
        public DisjointUnion(QueryPlan... sources) {
            super(sources);
        }

        @Override
        void appendTo(Appendable a, String in1, String in2) throws IOException {
            appendTo(a, in1, in2, "disjoint union");
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof DisjointUnion union && matches(union);
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ 672122059;
        }
    }

    /**
     * Query plan node which represents a union set of plans, where each source plan is limited
     * to a range, and all sources produce rows in the same order.
     */
    public static final class RangeUnion extends Union {
        private static final long serialVersionUID = 1L;

        /**
         * @param sources child plan nodes
         */
        public RangeUnion(QueryPlan... sources) {
            super(sources);
        }

        @Override
        void appendTo(Appendable a, String in1, String in2) throws IOException {
            appendTo(a, in1, in2, "range union");
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof RangeUnion union && matches(union);
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ -1637108271;
        }
    }

    /**
     * Query plan node which represents a union set of plans which have an common ordering, and
     * rows are compared to each other for eliminating duplicates.
     */
    public static final class MergeUnion extends Union {
        private static final long serialVersionUID = 1L;

        /**
         * @param sources child plan nodes
         */
        public MergeUnion(QueryPlan... sources) {
            super(sources);
        }

        @Override
        void appendTo(Appendable a, String in1, String in2) throws IOException {
            appendTo(a, in1, in2, "merge union");
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof MergeUnion union && matches(union);
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ 658549224;
        }
    }

    /**
     * Query plan node which represents a nested loop join.
     */
    public static final class NestedLoopsJoin extends Set {
        private static final long serialVersionUID = 1L;

        /**
         * @param levels ordered by outermost level to innermost level
         */
        public NestedLoopsJoin(Level... levels) {
            super(levels);
        }

        public static final class Level extends QueryPlan {
            public final String type;
            public final QueryPlan source;
            public final Map<Integer, String> assignments;

            /**
             * @param type join type from the previous level
             * @param source optional child plan node
             * @param assignments optional map of arguments assigned by this level for use by the
             * next and remaining levels
             */
            public Level(String type, QueryPlan source, Map<Integer, String> assignments) {
                this.type = type;
                this.source = source;
                this.assignments = assignments;
            }

            @Override
            void appendTo(Appendable a, String in1, String in2) throws IOException {
                a.append(in1).append(type).append('\n');

                if (source != null) {
                    appendSub(a, in2, null, source);
                }

                if (assignments != null && !assignments.isEmpty()) {
                    a.append(in2).append("assignments").append(": ");
                    int i = 0;
                    for (Map.Entry<Integer, String> e : assignments.entrySet()) {
                        if (i++ > 0) {
                            a.append(", ");
                        }
                        a.append('?').append(String.valueOf(e.getKey())).append(" = ")
                            .append(String.valueOf(e.getValue()));
                    }
                    a.append('\n');
                }
            }

            @Override
            public boolean equals(Object obj) {
                return obj instanceof Level level && matches(level);
            }

            boolean matches(Level other) {
                return Objects.equals(type, other.type)
                    && Objects.equals(source, other.source)
                    && Objects.equals(assignments, other.assignments);
            }

            @Override
            public int hashCode() {
                int hash = Objects.hashCode(type);
                hash = hash * 31 + Objects.hashCode(source);
                hash = hash * 31 + Objects.hashCode(assignments);
                return hash ^ 1463224650;
            }
        }

        @Override
        void appendTo(Appendable a, String in1, String in2) throws IOException {
            super.appendTo(a, in1, in2, "nested loops join");
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof NestedLoopsJoin join && matches(join);
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ 533558266;
        }
    }

    /**
     * Query plan node which represents a concatenated set of plans.
     */
    public static sealed class Concat extends Set {
        private static final long serialVersionUID = 1L;

        /**
         * @param sources child plan nodes
         */
        public Concat(QueryPlan... sources) {
            super(sources);
        }

        @Override
        void appendTo(Appendable a, String in1, String in2) throws IOException {
            appendTo(a, in1, in2, "concat");
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Concat concat && matches(concat);
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ -2085193523;
        }
    }

    /**
     * Query plan node which represents a concatenated set of plans which have an explicit
     * ordering, and source rows are compared to each other to maintain the ordering.
     */
    public static final class MergeConcat extends Concat {
        private static final long serialVersionUID = 1L;

        /**
         * @param sources child plan nodes
         */
        public MergeConcat(QueryPlan... sources) {
            super(sources);
        }

        @Override
        void appendTo(Appendable a, String in1, String in2) throws IOException {
            appendTo(a, in1, in2, "merge concat");
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof MergeConcat merge && matches(merge);
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ -481770497;
        }
    }
}
