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

package org.cojen.tupl.table;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import org.cojen.tupl.table.filter.AndFilter;
import org.cojen.tupl.table.filter.ColumnFilter;
import org.cojen.tupl.table.filter.ColumnToArgFilter;
import org.cojen.tupl.table.filter.ColumnToColumnFilter;
import org.cojen.tupl.table.filter.OrFilter;
import org.cojen.tupl.table.filter.QuerySpec;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.TrueFilter;
import org.cojen.tupl.table.filter.Visitor;

/**
 * Selects one or more indexes which are best suited for handling a query.
 *
 * @author Brian S O'Neill
 */
final class IndexSelector<R> {
    private final RowInfo mPrimaryInfo;
    private final QuerySpec mQuery;
    private final boolean mForUpdate;

    private NavigableSet<ColumnSet> mAlternateKeys;
    private NavigableSet<ColumnSet> mSecondaryIndexes;

    private long mAnyTermMatches;

    private boolean mAnyFirstOrderMatches;

    private boolean mMultipleSelections;

    private ColumnSet[] mSelectedIndexes;
    private StoredTable[] mSelectedIndexTables;
    private QuerySpec[] mSelectedQueries;
    private boolean[] mSelectedReverse;
    private OrderBy mOrderBy;
    private OrderBy mGrouping;

    private boolean mForUpdateRule;

    /**
     * @param table used to verify that the selected indexes are available; pass null to skip
     * verification
     * @throws IOException only can be thrown if a table was provided
     */
    IndexSelector(StoredTable<R> table, RowInfo primaryInfo, QuerySpec query, boolean forUpdate)
        throws IOException
    {
        mPrimaryInfo = primaryInfo;
        mQuery = query;
        mForUpdate = forUpdate;

        mAlternateKeys = primaryInfo.alternateKeys;
        mSecondaryIndexes = primaryInfo.secondaryIndexes;

        analyze: while (true) {
            analyze();

            if (table == null) {
                break;
            }

            StoredTable[] selectedIndexTables = new StoredTable[numSelected()];

            for (int i=0; i<selectedIndexTables.length; i++) {
                ColumnSet subIndex = selectedIndex(i);

                if (subIndex.matches(primaryInfo)) {
                    selectedIndexTables[i] = table;
                    continue;
                }

                StoredTable<R> subTable;

                if (mAlternateKeys.contains(subIndex)) {
                    // Alternate key.
                    try {
                        subTable = table.viewIndexTable(true, subIndex.keySpec());
                    } catch (NoSuchIndexException e) {
                        // Alternate key isn't available, so analyze again without it.
                        if (mAlternateKeys == primaryInfo.alternateKeys) {
                            mAlternateKeys = new TreeSet<>(mAlternateKeys);
                        }
                        mAlternateKeys.remove(subIndex);
                        continue analyze;
                    }
                } else {
                    // Secondary index.
                    try {
                        subTable = table.viewIndexTable(false, subIndex.fullSpec());
                    } catch (NoSuchIndexException e) {
                        // Secondary index isn't available, so analyze again without it.
                        if (mSecondaryIndexes == primaryInfo.secondaryIndexes) {
                            mSecondaryIndexes = new TreeSet<>(mSecondaryIndexes);
                        }
                        mSecondaryIndexes.remove(subIndex);
                        continue analyze;
                    }
                }

                selectedIndexTables[i] = subTable;
            }

            mSelectedIndexTables = selectedIndexTables;
            break;
        }
    }

    private void analyze() {
        selectIndexes();
        int num = numSelected();

        // Decide scan order, and also decide if sorting is required. Note that reduceOrdering
        // is called after index selection. This allows the orderBy to serve as an index
        // selection hint.

        OrderBy orderBy = mQuery.orderBy();

        if (orderBy != null) {
            orderBy = reduceOrdering(orderBy, mPrimaryInfo);
            if (orderBy != null) {
                for (ColumnSet key : mAlternateKeys) {
                    orderBy = reduceOrdering(orderBy, key);
                    if (orderBy == null) {
                        break;
                    }
                }
            }
        }

        mOrderBy = orderBy;

        if (orderBy == null || orderBy.isEmpty()) {
            for (int i=0; i<num; i++) {
                if (shouldReverseScan(i)) {
                    if (mSelectedReverse == null) {
                        mSelectedReverse = new boolean[num];
                    }
                    mSelectedReverse[i] = true;
                }
            }

            return;
        }

        mSelectedReverse = new boolean[num];

        forAllSelected: for (int i=0; i<num; i++) {
            Iterator<ColumnInfo> columns = mSelectedIndexes[i].keyColumns.values().iterator();
            Iterator<OrderBy.Rule> rules = orderBy.values().iterator();

            int direction = 0;
            int numMatches = 0;

            while (true) {
                if (!rules.hasNext()) {
                    // The index order (or reversed) fully matches the requested ordering.
                    if (num == 1) {
                        // If the matched index is the only one, then there's no need sort.
                        mOrderBy = null;
                        break forAllSelected;
                    }
                    continue forAllSelected;
                }

                if (!columns.hasNext()) {
                    // All of the key columns match, and so no more rules need to be examined.
                    if (num == 1) {
                        // If the matched index is the only one, then there's no need sort.
                        mOrderBy = null;
                        break forAllSelected;
                    }
                    break;
                }

                ColumnInfo column = columns.next();
                OrderBy.Rule rule = rules.next();

                int match = compareOrdering(column, rule, direction);

                if (match == 0) {
                    break;
                }

                if (direction == 0 && match < 0) {
                    // Decide scan order based on the first column only.
                    mSelectedReverse[i] = true;
                }

                direction = match;
                numMatches++;
            }

            if (num == 1) {
                mGrouping = orderBy.truncate(numMatches);
            }
        }
    }

    /**
     * Removes unnecessary ordering columns. Once the orderBy specification has incorporated
     * all columns of a primary or alternate key, no more ordering is necessary. In addition,
     * if the filter exactly specifies a column (with the '==' operator), it's removed from the
     * orderBy specification.
     *
     * @param orderBy cannot be null
     */
    private OrderBy reduceOrdering(OrderBy orderBy, ColumnSet key) {
        Map<String, ColumnInfo> keyColumns = key.keyColumns;

        int remaining = keyColumns.size();
        int num = 0;

        for (String name : orderBy.keySet()) {
            num++;
            if (keyColumns.containsKey(name) && --remaining <= 0) {
                break;
            }
        }

        orderBy = orderBy.truncate(num);

        if (orderBy == null) {
            return null;
        }

        RowFilter filter = mQuery.filter();
        boolean copied = false;

        for (String name : orderBy.keySet()) {
            if (filter.uniqueColumn(name)) {
                if (!copied) {
                    orderBy = new OrderBy(orderBy);
                    copied = true;
                }
                orderBy.remove(name);
            }
        }

        return orderBy.isEmpty() ? null : orderBy;
    }

    private void selectIndexes() {
        final ColumnSet theOne;

        one: {
            if (mAlternateKeys.isEmpty() && mSecondaryIndexes.isEmpty()) {
                theOne = mPrimaryInfo;
                break one;
            }
        
            RowFilter dnf = mQuery.filter().dnf();

            if (!(dnf instanceof OrFilter orf)) {
                if (dnf == TrueFilter.THE) {
                    theOne = findBestFullScanIndex();
                } else {
                    theOne = selectIndex(dnf);
                }
                break one;
            }

            var selections = new LinkedHashMap<ColumnSet, RowFilter>();

            selectAll: while (true) {
                boolean fullScan = false;

                for (RowFilter group : orf.subFilters()) {
                    ColumnSet index = selectIndex(group);
                    fullScan |= mAnyTermMatches == 0;

                    RowFilter existing = selections.get(index);
                    if (existing == null) {
                        selections.put(index, group);
                    } else {
                        selections.put(index, existing.or(group).reduce());
                    }

                    if (selections.size() > 1) {
                        if (fullScan) {
                            // If a full scan of at least one index is required, and multiple
                            // indexes are selected, then always do a full scan of the best
                            // covering index instead.
                            theOne = findBestFullScanIndex();
                            break one;
                        }

                        mMultipleSelections = true;

                        if (mAnyFirstOrderMatches) {
                            // A call to isFirstOrderByColumn returned true, but now that it's
                            // known that multiple indexes are to be selected, the call to
                            // isFirstOrderByColumn should have returned false. Start over.
                            mAnyFirstOrderMatches = false;
                            selections.clear();
                            continue selectAll;
                        }
                    }
                }

                break;
            }

            if (selections.size() == 1) {
                theOne = selections.keySet().iterator().next();
                break one;
            }

            if (selections.isEmpty()) {
                theOne = mPrimaryInfo;
                break one;
            }

            // All selected filters must be disjoint. Iterate over them ordered by the number
            // of filter terms, to help reduce the size of the disjoint filters.

            var entries = new ArrayList<>(selections.entrySet());
            entries.sort(Comparator.comparingInt(e -> e.getValue().numTerms()));

            mSelectedIndexes = new ColumnSet[entries.size()];
            mSelectedQueries = new QuerySpec[mSelectedIndexes.length];

            Iterator<Map.Entry<ColumnSet, RowFilter>> it = entries.iterator();
            RowFilter reject = null;

            for (int i=0; i<mSelectedIndexes.length; i++) {
                Map.Entry<ColumnSet, RowFilter> selected = it.next();
                mSelectedIndexes[i] = selected.getKey();
                RowFilter filter = selected.getValue();
                if (reject == null) {
                    reject = filter.not();
                } else {
                    RowFilter disjoint = filter.and(reject).reduce();
                    reject = reject.and(filter.not());
                    filter = disjoint;
                }
                mSelectedQueries[i] = mQuery.withFilter(filter).withOrderBy(null);
            }

            return;
        }

        // Reached when only one index was selected.
        mSelectedIndexes = new ColumnSet[] {theOne};
        mSelectedQueries = new QuerySpec[] {mQuery.withOrderBy(null)};
    }

    /**
     * Returns the constructor parameter.
     */
    RowInfo primaryInfo() {
        return mPrimaryInfo;
    }

    /**
     * Returns the constructor parameter.
     */
    QuerySpec query() {
        return mQuery;
    }

    /**
     * Returns the constructor parameter.
     */
    boolean forUpdate() {
        return mForUpdate;
    }

    /**
     * Returns the number of selected indexes.
     */
    int numSelected() {
        return mSelectedIndexes.length;
    }

    ColumnSet selectedIndex(int i) {
        return mSelectedIndexes[i];
    }

    /**
     * @throws NullPointerException if no base table was provided to the constructor
     */
    @SuppressWarnings("unchecked")
    StoredTable<R> selectedIndexTable(int i) {
        return mSelectedIndexTables[i];
    }

    /**
     * Returns a query for the selected index with the effective projection and filter that
     * must still be applied.
     */
    QuerySpec selectedQuery(int i) {
        return mSelectedQueries[i];
    }

    /**
     * Returns true if index scan should go in reverse order.
     */
    boolean selectedReverse(int i) {
        return mSelectedReverse != null && mSelectedReverse[i];
    }

    /**
     * Returns null if no sort step is required.
     */
    OrderBy orderBy() {
        return mOrderBy;
    }

    /**
     * If a sort step is required, the grouping represents the natural partial ordering before
     * sorting. If no sort step is required, then null is returned.
     */
    OrderBy grouping() {
        return mGrouping;
    }

    Set<String> projection() {
        Map<String, ColumnInfo> projection = mQuery.projection();
        return projection == null ? null : projection.keySet();
    }

    /**
     * Returns true if the forUpdate parameter was true and it had an effect on index selection.
     */
    boolean forUpdateRuleChosen() {
        return mForUpdateRule;
    }

    /**
     * Returns true if none of the selected indexes need to join to a primary. Projection isn't
     * considered because even when a selected index is covering, a join might still be needed
     * if a double check filter is needed against the primary row.
     *
     * @throws NullPointerException if no base table was provided to the constructor
     */
    boolean noJoins() {
        int numSelected = numSelected();
        for (int i=0; i<numSelected; i++) {
            if (selectedIndexTable(i).joinedPrimaryTableClass() != null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if an open range scan should scan in reverse, which is more efficient because no
     * predicate needs to be checked for each row.
     */
    private boolean shouldReverseScan(int i) {
        RowFilter rf = mSelectedQueries[i].filter();
        if (!(rf instanceof ColumnToArgFilter cf)) {
            return false;
        }

        int descending;

        switch (cf.operator()) {
            case ColumnFilter.OP_LT, ColumnFilter.OP_LE -> {
                descending = 0;
            }
            case ColumnFilter.OP_GT, ColumnFilter.OP_GE -> {
                descending = ColumnInfo.TYPE_DESCENDING;
            }
            default -> {
                return false;
            }
        }

        ColumnInfo ixColumn = mSelectedIndexes[i].keyColumns.values().iterator().next();

        if ((ixColumn.typeCode & ColumnInfo.TYPE_DESCENDING) != descending) {
            return false;
        }

        return ixColumn.name.equals(cf.column().name);
    }

    /**
     * @param group must be a single DNF group; no "or" filters
     */
    private ColumnSet selectIndex(RowFilter group) {
        mAnyTermMatches = 0;
        var terms = makeTerms(group);
        ColumnSet best = mPrimaryInfo;

        for (ColumnSet cs : mAlternateKeys) {
            if (compareIndexes(group, terms, cs, best) < 0) {
                best = cs;
            }
        }

        for (ColumnSet cs : mSecondaryIndexes) {
            if (compareIndexes(group, terms, cs, best) < 0) {
                best = cs;
            }
        }

        return best;
    }

    /**
     * Returns -1 if cs1 is better than cs2, ...
     *
     * @param group must be a single DNF group; no "or" filters
     */
    private int compareIndexes(RowFilter group, List<Term> terms, ColumnSet cs1, ColumnSet cs2) {
        // Select an index based on how well the key columns match.
        int cmp = Long.compare(keyMatchScore(terms, cs2), keyMatchScore(terms, cs1));
        if (cmp != 0) {
            return cmp;
        }

        // Select an index which contains all filtering and projected columns, thus averting a join.
        cmp = compareCovering(terms, cs1, cs2, false);
        if (cmp != 0) {
            return cmp;
        }

        // Select an index based on how many columns it has available for filtering.
        cmp = Integer.compare(columnAvailability(terms, cs2), columnAvailability(terms, cs1));
        if (cmp != 0) {
            return cmp;
        }

        // Select an index based on how well its natural order matches the requested ordering.
        cmp = compareOrdering(cs1, cs2);
        if (cmp != 0) {
            return cmp;
        }

        // Select an index based on the order in which its key columns appear in the filter.
        // This is the closest thing to an index selection "hint".
        cmp = comparePreference(group, cs1, cs2);
        if (cmp != 0) {
            return cmp;
        }

        // Select the index with the fewest number of columns.
        return Integer.compare(cs1.allColumns.size(), cs2.allColumns.size());
    }

    /**
     * Returns a score which is higher for indexes that are better suited for key matching.
     *
     * @param terms must be sorted
     */
    private long keyMatchScore(List<Term> terms, ColumnSet cs) {
        long score = 0;
        int primaryMatches = 0;

        scan: for (ColumnInfo column : cs.keyColumns.values()) {
            Term term;
            findTerm: {
                for (Term t : terms) {
                    if (t.mType > HALF_RANGE) {
                        break scan;
                    }
                    if (t.mFilter.column().name.equals(column.name)) {
                        term = t;
                        break findTerm;
                    }
                }

                break scan;
            }

            switch (term.mType) {
                case EQUALITY -> {
                    if (cs != mPrimaryInfo
                        && mPrimaryInfo.keyColumns.containsKey(column.name)
                        && ++primaryMatches == mPrimaryInfo.keyColumns.size())
                    {
                        // If an index has exactly matched all of the primary key columns, then
                        // peg the score to match that of the primary index. This prevents it
                        // from being immediately ranked higher than the primary index.
                        score = primaryMatches * 3L;
                        break scan;
                    }
                    score += 3;
                    continue;
                }
                case FULL_RANGE -> {
                    score += 2;
                }
                case HALF_RANGE -> {
                    if (score > 0 || isCovering(terms, cs) || isFirstOrderByColumn(column)) {
                        // Only consider a half range match after the first index column, or if
                        // no join is required, or if the column is the first for ordering,
                        score += 1;
                    }
                }
            }

            // Stop checking upon reaching a range match.
            break;
        }

        // This field is only compared against zero, and so the absolute value doesn't matter.
        mAnyTermMatches += score;

        return score;
    }

    private boolean isFirstOrderByColumn(ColumnInfo column) {
        if (mMultipleSelections) {
            // If an ordering is requested, and multiple indexes are selected, a sort must be
            // performed, so don't select an index based on natural order.
            return false;
        }

        OrderBy orderBy = mQuery.orderBy();
        if (orderBy == null || orderBy.isEmpty()
            || compareOrdering(column, orderBy.values().iterator().next()) == 0)
        {
            return false;
        }

        mAnyFirstOrderMatches = true;

        return true;
    }

    /**
     * @return 0 if not matched, 1 if matched exactly, or -1 if matched with a flipped direction
     */
    private static int compareOrdering(ColumnInfo column, OrderBy.Rule rule) {
        int rt;
        if (!column.name.equals(rule.column().name)
            || column.unorderedTypeCode() != ColumnInfo.unorderedTypeCode(rt = rule.type()))
        {
            return 0;
        } else {
            return column.isDescending() == ColumnInfo.isDescending(rt) ? 1 : -1;
        }
    }

    /**
     * @param direction match results for previous columns, or 0 if this is the first column
     * @return 0 if not matched, 1 if matched exactly, or -1 if matched with a flipped direction
     */
    private static int compareOrdering(ColumnInfo column, OrderBy.Rule rule, int direction) {
        int cmp = compareOrdering(column, rule);
        return (cmp == 0 || (direction != 0 && direction != cmp)) ? 0 : cmp;
    }

    /**
     * Returns true if the given index (cs) contains all the required columns.
     */
    private boolean isCovering(List<Term> terms, ColumnSet cs) {
        if (cs == mPrimaryInfo) {
            return true;
        }

        Map<String, ColumnInfo> pmap = mQuery.projection();
        if (pmap == null) {
            pmap = mPrimaryInfo.allColumns;
        }

        if (!cs.allColumns.keySet().containsAll(pmap.keySet())) {
            return false;
        }

        for (Term term : terms) {
            ColumnFilter filter = term.mFilter;
            if (!cs.allColumns.containsKey(filter.column().name)) {
                return false;
            }
            if (filter instanceof ColumnToColumnFilter ctc) {
                if (!cs.allColumns.containsKey(ctc.otherColumn().name)) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Returns -1 if cs1 is better than cs2, ...
     * @param ordering pass true to also consider natural order when neither set is covering
     */
    private int compareCovering(List<Term> terms, ColumnSet cs1, ColumnSet cs2, boolean ordering) {
        Map<String, ColumnInfo> pmap = mQuery.projection();

        Set<String> required;
        if (pmap == null) {
            required = mPrimaryInfo.allColumns.keySet();
        } else {
            required = new HashSet<>(pmap.keySet());
            for (Term term : terms) {
                ColumnFilter filter = term.mFilter;
                required.add(filter.column().name);
                if (filter instanceof ColumnToColumnFilter ctc) {
                    required.add(ctc.otherColumn().name);
                }
            }
        }

        return compareCovering(required, cs1, cs2, ordering);
    }

    /**
     * Returns -1 if cs1 is better than cs2, ...
     *
     * @param required set of column names
     * @param ordering pass true to also consider natural order when neither set is covering
     */
    private int compareCovering(Set<String> required, ColumnSet cs1, ColumnSet cs2,
                                boolean ordering)
    {
        if (cs1.allColumns.keySet().containsAll(required)) {
            if (cs2.allColumns.keySet().containsAll(required)) {
                int cmp = compareOrdering(cs1, cs2);
                if (cmp == 0) {
                    cmp = Integer.compare(cs1.allColumns.size(), cs2.allColumns.size());
                }
                return cmp;
            } else {
                return -1;
            }
        } else if (cs2.allColumns.keySet().containsAll(required)) {
            return 1;
        } else if (ordering) {
            return compareOrdering(cs1, cs2);
        } else {
            return 0;
        }
    }

    /**
     * Returns the number of requested filtering columns which are available in an index.
     */
    private static int columnAvailability(List<Term> terms, ColumnSet cs) {
        var available = new HashSet<String>();
        Map<String, ColumnInfo> columns = cs.allColumns;
        for (Term term : terms) {
            ColumnFilter filter = term.mFilter;
            String name = filter.column().name;
            if (columns.containsKey(name)) {
                available.add(name);
            }
            if (filter instanceof ColumnToColumnFilter ctc) {
                name = ctc.otherColumn().name;
                if (columns.containsKey(name)) {
                    available.add(name);
                }
            }
        }
        return available.size();
    }

    /**
     * Returns -1 if cs1 is better than cs2, ...
     *
     * @param group must be a single DNF group; no "or" filters
     */
    private static int comparePreference(RowFilter group, ColumnSet cs1, ColumnSet cs2) {
        if (group instanceof ColumnFilter cf) {
            return comparePreference(cf, cs1, cs2);
        }

        if (group instanceof AndFilter andf) {
            for (RowFilter sub : andf.subFilters()) {
                if (sub instanceof ColumnFilter cf) {
                    int cmp = comparePreference(cf, cs1, cs2);
                    if (cmp != 0) {
                        return cmp;
                    }
                }
            }
        }

        return 0;
    }

    /**
     * Returns -1 if cs1 is better than cs2, ...
     */
    private static int comparePreference(ColumnFilter cf, ColumnSet cs1, ColumnSet cs2) {
        String name = cf.column().name;

        Iterator<String> it1 = cs1.keyColumns.keySet().iterator();
        Iterator<String> it2 = cs2.keyColumns.keySet().iterator();

        while (it1.hasNext() && it2.hasNext()) {
            String key1 = it1.next();
            String key2 = it2.next();
            if (name.equals(key1)) {
                return name.equals(key2) ? 0 : -1;
            }
            if (name.equals(key2)) {
                return 1;
            }
        }

        return 0;
    }

    /**
     * Returns -1 if cs1 is better than cs2, ...
     */
    private int compareOrdering(ColumnSet cs1, ColumnSet cs2) {
        OrderBy orderBy = mQuery.orderBy();
        if (orderBy == null || orderBy.isEmpty()) {
            return 0;
        }

        Iterator<OrderBy.Rule> rules = orderBy.values().iterator();
        Iterator<ColumnInfo> it1 = cs1.keyColumns.values().iterator();
        Iterator<ColumnInfo> it2 = cs2.keyColumns.values().iterator();

        int dir1 = 0;
        int dir2 = 0;

        while (rules.hasNext() && it1.hasNext() && it2.hasNext()) {
            OrderBy.Rule rule = rules.next();

            ColumnInfo col1 = it1.next();
            ColumnInfo col2 = it2.next();

            dir1 = compareOrdering(col1, rule, dir1);
            dir2 = compareOrdering(col2, rule, dir2);

            if (dir1 == 0) {
                return dir2 == 0 ? 0 : 1;
            } else if (dir2 == 0) {
                return -1;
            }
        }

        return 0;
    }

    private ColumnSet findBestFullScanIndex() {
        ColumnSet best = mPrimaryInfo;

        // The primary index is best if there's no filter and the requested ordering matches at
        // least the first column.

        if (mQuery.filter() == TrueFilter.THE) {
            OrderBy orderBy = mQuery.orderBy();
            if (orderBy != null && !orderBy.isEmpty()) {
                ColumnInfo firstColumn = best.keyColumns.values().iterator().next();
                OrderBy.Rule firstOrderBy = orderBy.values().iterator().next();
                if (compareOrdering(firstColumn, firstOrderBy) != 0) {
                    return best;
                }
            }
        }

        // The best contains all projected columns and all filter terms, but with the fewest
        // extraneous columns. When all are required, the best is the primary index.

        Map<String, ColumnInfo> pmap = mQuery.projection(); // is null if all are required

        if (pmap != null) {
            var required = new HashSet<>(pmap.keySet());

            mQuery.filter().accept(new Visitor() {
                @Override
                public void visit(ColumnToArgFilter filter) {
                    required.add(filter.column().name);
                }

                @Override
                public void visit(ColumnToColumnFilter filter) {
                    required.add(filter.column().name);
                    required.add(filter.otherColumn().name);
                }
            });

            for (ColumnSet cs : mAlternateKeys) {
                if (compareCovering(required, cs, best, true) < 0) {
                    best = cs;
                }
            }

            for (ColumnSet cs : mSecondaryIndexes) {
                if (compareCovering(required, cs, best, true) < 0) {
                    best = cs;
                }
            }
        }

        if (mForUpdate && best != mPrimaryInfo) {
            // Always choose the primary index, avoiding a join.
            best = mPrimaryInfo;
            mForUpdateRule = true;
        }

        return best;
    }

    /**
     * Returns a sorted list of terms.
     *
     * @param group must be a single DNF group; no "or" filters
     */
    private static List<Term> makeTerms(RowFilter group) {
        if (group instanceof ColumnFilter cf) {
            Term term = makeTerm(cf);
            if (term.mType == CANDIDATE) {
                term.mType = HALF_RANGE;
            }
            return List.of(term);
        }

        if (!(group instanceof AndFilter andf)) {
            throw new AssertionError();
        }

        RowFilter[] subFilters = andf.subFilters();
        List<Term> terms = new ArrayList<>(subFilters.length);
        List<Term> candidates = null;

        for (RowFilter sub : subFilters) {
            if (!(sub instanceof ColumnFilter cf)) {
                throw new AssertionError();
            }
            Term term = makeTerm(cf);
            if (term.mType != CANDIDATE) {
                terms.add(term);
            } else {
                if (candidates == null) {
                    candidates = new ArrayList<>(subFilters.length >> 1);
                }
                candidates.add(term);
            }
        }

        Collections.sort(terms);

        if (candidates == null) {
            return terms;
        }

        // Handle any CANDIDATE terms, by converting them to HALF_RANGE terms or by merging
        // them with other HALF_RANGE terms, thereby making them FULL_RANGE terms.

        int scanEnd = terms.size();
        int scanStart = 0;
        outer: for (; scanStart < scanEnd; scanStart++) {
            if (terms.get(scanStart).mType == HALF_RANGE) {
                for (int i = scanStart + 1; i < scanEnd; i++) {
                    if (terms.get(i).mType > HALF_RANGE) {
                        scanEnd = i;
                        break outer;
                    }
                }
                break;
            }
        }

        outer: for (Term candidate : candidates) {
            for (int i = scanStart; i < scanEnd; i++) {
                Term term = terms.get(i);
                if (term.mType == HALF_RANGE &&
                    term.mFilter.column().equals(candidate.mFilter.column()))
                {
                    term.mType = FULL_RANGE;
                    term.mCompanion = candidate.mFilter;
                    continue outer;
                }
            }

            candidate.mType = HALF_RANGE;
            terms.add(candidate);
        }

        // Terms changed, so sort them again.
        Collections.sort(terms);

        return terms;
    }

    /**
     * Returns a term which is any type but FULL_RANGE.
     */
    private static Term makeTerm(ColumnFilter filter) {
        int type = REMAINDER;

        if (filter instanceof ColumnToArgFilter cf) {
            switch (cf.operator()) {
                case ColumnFilter.OP_EQ -> type = EQUALITY;
                case ColumnFilter.OP_GT, ColumnFilter.OP_GE -> type = HALF_RANGE;
                case ColumnFilter.OP_LT, ColumnFilter.OP_LE -> type = CANDIDATE;
            }
        }

        return new Term(type, filter);
    }

    // Term types.
    private static final int
        EQUALITY = 1, FULL_RANGE = 2, HALF_RANGE = 3, REMAINDER = 4, CANDIDATE = 5;

    /**
     * Terms are ordered by overall reduction power. Lower is better.
     */
    private static class Term implements Comparable<Term> {
        int mType;
        ColumnFilter mFilter;
        ColumnFilter mCompanion; // applicable to FULL_RANGE only

        Term(int type, ColumnFilter filter) {
            mType = type;
            mFilter = filter;
        }

        @Override
        public int compareTo(Term other) {
            int cmp = Integer.compare(mType, other.mType);

            if (cmp == 0) {
                cmp = Integer.compare(opOrder(mFilter), opOrder(other.mFilter));
                if (cmp == 0 && mCompanion != null && other.mCompanion != null) {
                    cmp = Integer.compare(opOrder(mCompanion), opOrder(other.mCompanion));
                }
            }

            return cmp;
        }

        private static int opOrder(ColumnFilter filter) {
            return switch (filter.operator()) {
                case ColumnFilter.OP_EQ -> 1;
                case ColumnFilter.OP_GT, ColumnFilter.OP_LT,
                     ColumnFilter.OP_GE, ColumnFilter.OP_LE -> 2;
                case ColumnFilter.OP_NE -> 3;
                case ColumnFilter.OP_IN -> 4;
                default -> 5;
            };
        }
    }
}
