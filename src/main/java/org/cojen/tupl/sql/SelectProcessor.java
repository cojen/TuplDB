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

package org.cojen.tupl.sql;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;

import net.sf.jsqlparser.statement.select.*;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.model.BinaryOpNode;
import org.cojen.tupl.model.JoinNode;
import org.cojen.tupl.model.Node;
import org.cojen.tupl.model.RelationNode;
import org.cojen.tupl.model.SelectNode;

import static org.cojen.tupl.rows.join.JoinSpec.*;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class SelectProcessor implements SelectVisitor {
    /**
     * @param finder used for finding fully functional Table instances
     */
    public static RelationNode process(Select select, TableFinder finder) {
        var processor = new SelectProcessor(finder);
        select.accept(processor);
        return processor.mNode;
    }

    private final TableFinder mFinder;

    private RelationNode mNode;

    private SelectProcessor(TableFinder finder) {
        mFinder = finder;
    }

    @Override
    public void visit(ParenthesedSelect parenthesedSelect) {
        fail();
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        if (plainSelect.getFetch() != null ||
            plainSelect.getForClause() != null ||
            plainSelect.getIsolation() != null ||
            plainSelect.getLimit() != null ||
            plainSelect.getLimitBy() != null ||
            plainSelect.getOffset() != null ||
            plainSelect.getOrderByElements() != null ||
            plainSelect.getWithItemsList() != null ||
            plainSelect.isOracleSiblings() ||
            plainSelect.getDistinct() != null ||
            plainSelect.getFirst() != null ||
            plainSelect.getForMode() != null ||
            plainSelect.getForUpdateTable() != null ||
            plainSelect.getForXmlPath() != null ||
            plainSelect.getGroupBy() != null ||
            plainSelect.getHaving() != null ||
            plainSelect.getIntoTables() != null ||
            plainSelect.getKsqlWindow() != null ||
            plainSelect.getLateralViews() != null ||
            plainSelect.getMySqlSqlCalcFoundRows() ||
            plainSelect.getOracleHierarchical() != null ||
            plainSelect.getQualify() != null ||
            plainSelect.getSkip() != null ||
            plainSelect.getTop() != null ||
            plainSelect.getWait() != null ||
            plainSelect.getWindowDefinitions() != null)
        {
            throw fail();
        }

        RelationNode from = null;

        FromItem fromItem = plainSelect.getFromItem();
        if (fromItem != null) {
            try {
                from = FromProcessor.process(plainSelect.getFromItem(), mFinder);
            } catch (Throwable e) {
                throw Utils.rethrow(e);
            }
        }

        List<Join> joins = plainSelect.getJoins();

        if (joins != null) for (Join join : joins) {
            if (join.getJoinWindow() != null ||
                join.getUsingColumns() != null && (!join.getUsingColumns().isEmpty()) ||
                join.isApply() ||
                join.isGlobal() ||
                join.isNatural() ||
                join.isSemi() ||
                join.isWindowJoin())
            {
                throw fail();
            }

            RelationNode fromRight;
            try {
                fromRight = FromProcessor.process(join.getFromItem(), mFinder);
            } catch (Throwable e) {
                throw Utils.rethrow(e);
            }

            int joinType;

            if (join.isInnerJoin() || join.isCross()) {
                joinType = join.isStraight() ? T_STRAIGHT : T_INNER;
            } else if (join.isLeft()) {
                joinType = T_LEFT_OUTER;
            } else if (join.isRight()) {
                joinType = T_RIGHT_OUTER;
            } else if (join.isFull()) {
                joinType = T_FULL_OUTER;
            } else {
                joinType = T_INNER;
            }

            from = JoinNode.make(null, joinType, from, fromRight);
        }

        Node where = null;

        if (joins != null) for (Join join : joins) {
            for (Expression on : join.getOnExpressions()) {
                where = andWhere(where, ExpressionProcessor.process(on, from));
            }
        }

        Expression whereExpr = plainSelect.getWhere();
        if (whereExpr != null) {
            where = andWhere(where, ExpressionProcessor.process(whereExpr, from));
        }

        List<SelectItem<?>> items = plainSelect.getSelectItems();

        List<Node> projection;
        if (items == null) {
            projection = List.of();
        } else {
            projection = new ArrayList<>();
            for (SelectItem item : items) {
                addToProjection(projection, item, from);
            }
        }

        mNode = SelectNode.make(null, from, where, projection.toArray(Node[]::new));
    }

    private static Node andWhere(Node where, Node whereMore) {
        if (where == null) {
            return whereMore;
        }
        return BinaryOpNode.make(null, BinaryOpNode.OP_AND, where, whereMore);
    }

    private static void addToProjection(List<Node> projection, SelectItem item, RelationNode from) {
        Expression expr = item.getExpression();
        Alias alias = item.getAlias();

        if (expr instanceof AllColumns ac) {
            if (alias != null) {
                throw fail();
            }
            if (from != null) {
                if (ac instanceof AllTableColumns atc) {
                    from.allTableColumns(projection, atc.getTable().getFullyQualifiedName());
                } else {
                    from.allColumns(projection);
                }
            }
            return;
        }

        Node node = ExpressionProcessor.process(expr, from);

        if (alias != null) {
            if (alias.getAliasColumns() != null) {
                throw fail();
            }
            node = node.withName(SqlUtils.unquote(alias.getName()));
        }

        projection.add(node);
    }

    @Override
    public void visit(SetOperationList setOpList) {
        fail();
    }

    @Override
    public void visit(WithItem withItem) {
        fail();
    }

    @Override
    public void visit(Values aThis) {
        fail();
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect) {
        fail();
    }

    @Override
    public void visit(TableStatement tableStatement) {
        fail();
    }

    private static UnsupportedOperationException fail() {
        throw new UnsupportedOperationException();
    }
}
