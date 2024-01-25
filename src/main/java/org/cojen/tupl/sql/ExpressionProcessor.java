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

import java.util.List;

import net.sf.jsqlparser.expression.*;

import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import net.sf.jsqlparser.statement.select.*;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.model.BinaryOpNode;
import org.cojen.tupl.model.CaseNode;
import org.cojen.tupl.model.ConcatNode;
import org.cojen.tupl.model.ConstantNode;
import org.cojen.tupl.model.Node;
import org.cojen.tupl.model.ParamNode;
import org.cojen.tupl.model.RelationNode;

import static org.cojen.tupl.model.BinaryOpNode.*;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class ExpressionProcessor implements ExpressionVisitor {
    /**
     * @param from used for finding columns
     */
    public static Node process(Expression expr, Scope scope) {
        var processor = new ExpressionProcessor(scope);
        expr.accept(processor);
        return processor.mNode;
    }

    private final Scope mScope;

    private Node mNode;

    private ExpressionProcessor(Scope scope) {
        mScope = scope;
    }

    @Override
    public void visit(BitwiseRightShift aThis) {
        fail();
    }

    @Override
    public void visit(BitwiseLeftShift aThis) {
        fail();
    }

    @Override
    public void visit(NullValue nullValue) {
        mNode = ConstantNode.NULL;
    }

    @Override
    public void visit(Function function) {
        fail();
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        signedExpression.getExpression().accept(this);
        char sign = signedExpression.getSign();
        if (sign == '-') {
            mNode = mNode.negate();
        } else if (sign != '+') {
            fail();
        }
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        mNode = ParamNode.make(jdbcParameter.getIndex());
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        fail();
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        mNode = ConstantNode.make(doubleValue.getValue());
    }

    @Override
    public void visit(LongValue longValue) {
        try {
            long longv = longValue.getValue();
            int intv = (int) longv;
            if (((long) intv) == longv) {
                mNode = ConstantNode.make(intv);
            } else {
                mNode = ConstantNode.make(longv);
            }
        } catch (NumberFormatException e) {
            mNode = ConstantNode.make(longValue.getBigIntegerValue());
        }
    }

    @Override
    public void visit(HexValue hexValue) {
        fail();
    }

    @Override
    public void visit(DateValue dateValue) {
        fail();
    }

    @Override
    public void visit(TimeValue timeValue) {
        fail();
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        fail();
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(StringValue stringValue) {
        if (stringValue.getPrefix() != null) {
            throw fail();
        }
        String str = stringValue.getValue();
        if (str.indexOf("''") >= 0) {
            str = str.replace("''", "'");
        }
        mNode = ConstantNode.make(str);
    }

    @Override
    public void visit(Addition addition) {
        visitBinaryOp(addition, OP_ADD);
    }

    @Override
    public void visit(Division division) {
        visitBinaryOp(division, OP_DIV);
    }

    @Override
    public void visit(IntegerDivision division) {
        fail();
    }

    @Override
    public void visit(Multiplication multiplication) {
        visitBinaryOp(multiplication, OP_MUL);
    }

    @Override
    public void visit(Subtraction subtraction) {
        visitBinaryOp(subtraction, OP_SUB);
    }

    @Override
    public void visit(AndExpression andExpression) {
        visitBinaryOp(andExpression, OP_AND);
    }

    @Override
    public void visit(OrExpression orExpression) {
        visitBinaryOp(orExpression, OP_OR);
    }

    @Override
    public void visit(XorExpression orExpression) {
        visitBinaryOp(orExpression, OP_XOR);
    }

    @Override
    public void visit(Between between) {
        fail();
    }

    @Override
    public void visit(OverlapsCondition overlapsCondition) {
        fail();
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        visitBinaryOp(equalsTo, OP_EQ);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        visitBinaryOp(greaterThan, OP_GT);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        visitBinaryOp(greaterThanEquals, OP_GE);
    }

    @Override
    public void visit(InExpression inExpression) {
        fail();
    }

    @Override
    public void visit(FullTextSearch fullTextSearch) {
        fail();
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        isNullExpression.getLeftExpression().accept(this);
        Node left = mNode;

        int op;
        if (isNullExpression.isUseNotNull()) {
            op = isNullExpression.isNot() ? OP_EQ : OP_NE;
        } else {
            op = isNullExpression.isNot() ? OP_NE : OP_EQ;
        }

        mNode = BinaryOpNode.make(op, left, ConstantNode.NULL);
    }

    @Override
    public void visit(IsBooleanExpression isBooleanExpression) {
        isBooleanExpression.getLeftExpression().accept(this);
        Node left = mNode;

        boolean value = isBooleanExpression.isTrue();
        int op = isBooleanExpression.isNot() ? OP_NE : OP_EQ;

        mNode = BinaryOpNode.make(op, left, ConstantNode.make(value));
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        fail();
    }

    @Override
    public void visit(MinorThan minorThan) {
        visitBinaryOp(minorThan, OP_LT);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        visitBinaryOp(minorThanEquals, OP_LE);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        visitBinaryOp(notEqualsTo, OP_NE);
    }

    @Override
    public void visit(DoubleAnd doubleAnd) {
        fail();
    }

    @Override
    public void visit(Contains contains) {
        fail();
    }

    @Override
    public void visit(ContainedBy containedBy) {
        fail();
    }

    @Override
    public void visit(ParenthesedSelect selectBody) {
        fail();
    }

    @Override
    public void visit(Column tableColumn) {
        if (tableColumn.getArrayConstructor() != null) {
            throw fail();
        }

        String name = tableColumn.getColumnName();

        Table table = tableColumn.getTable();
        if (table != null) {
            name = table.getFullyQualifiedName() + '.' + name;
        }

        mNode = mScope.findColumn(name);
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        Node switchNode;
        if (caseExpression.getSwitchExpression() == null) {
            switchNode = null;
        } else {
            caseExpression.getSwitchExpression().accept(this);
            switchNode = mNode;
        }

        List<WhenClause> whenClauses = caseExpression.getWhenClauses();
        var conditions = new Node[whenClauses.size()];
        var results = new Node[conditions.length];

        int i = 0;
        for (WhenClause when : whenClauses) {
            when.getWhenExpression().accept(this);
            Node whenNode = mNode;
            if (switchNode != null) {
                whenNode = BinaryOpNode.make(OP_EQ, switchNode, whenNode);
            }
            conditions[i] = whenNode;

            when.getThenExpression().accept(this);
            results[i] = mNode;

            i++;
        }

        Node elseResult;

        if (caseExpression.getElseExpression() == null) {
            elseResult = null;
        } else {
            caseExpression.getElseExpression().accept(this);
            elseResult = mNode;
        }

        mNode = CaseNode.make(conditions, results, elseResult);
    }

    @Override
    public void visit(WhenClause whenClause) {
        fail();
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        fail();
    }

    @Override
    public void visit(MemberOfExpression memberOfExpression) {
        fail();
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        fail();
    }

    @Override
    public void visit(Concat concat) {
        concat.getLeftExpression().accept(this);
        Node left = mNode;
        concat.getRightExpression().accept(this);
        Node right = mNode;
        mNode = ConcatNode.make(left, right);
    }

    @Override
    public void visit(Matches matches) {
        fail();
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        fail();
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        fail();
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        fail();
    }

    @Override
    public void visit(CastExpression cast) {
        fail();
    }

    @Override
    public void visit(Modulo modulo) {
        visitBinaryOp(modulo, OP_REM);
    }

    @Override
    public void visit(AnalyticExpression aexpr) {
        fail();
    }

    @Override
    public void visit(ExtractExpression eexpr) {
        fail();
    }

    @Override
    public void visit(IntervalExpression iexpr) {
        fail();
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) {
        fail();
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {
        fail();
    }

    @Override
    public void visit(JsonExpression jsonExpr) {
        fail();
    }

    @Override
    public void visit(JsonOperator jsonExpr) {
        fail();
    }

    @Override
    public void visit(UserVariable var) {
        fail();
    }

    @Override
    public void visit(NumericBind bind) {
        fail();
    }

    @Override
    public void visit(KeepExpression aexpr) {
        fail();
    }

    @Override
    public void visit(MySQLGroupConcat groupConcat) {
        fail();
    }

    @Override
    public void visit(ExpressionList<?> expressionList) {
        fail();
    }

    @Override
    public void visit(RowConstructor<?> rowConstructor) {
        fail();
    }

    @Override
    public void visit(RowGetExpression rowGetExpression) {
        fail();
    }

    @Override
    public void visit(OracleHint hint) {
        // Ignore.
    }

    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {
        fail();
    }

    @Override
    public void visit(DateTimeLiteralExpression literal) {
        fail();
    }

    @Override
    public void visit(NotExpression aThis) {
        aThis.getExpression().accept(this);
        mNode = mNode.not();
    }

    @Override
    public void visit(NextValExpression aThis) {
        fail();
    }

    @Override
    public void visit(CollateExpression aThis) {
        fail();
    }

    @Override
    public void visit(SimilarToExpression aThis) {
        fail();
    }

    @Override
    public void visit(ArrayExpression aThis) {
        fail();
    }

    @Override
    public void visit(ArrayConstructor aThis) {
        fail();
    }

    @Override
    public void visit(VariableAssignment aThis) {
        fail();
    }

    @Override
    public void visit(XMLSerializeExpr aThis) {
        fail();
    }

    @Override
    public void visit(TimezoneExpression aThis) {
        fail();
    }

    @Override
    public void visit(JsonAggregateFunction aThis) {
        fail();
    }

    @Override
    public void visit(JsonFunction aThis) {
        fail();
    }

    @Override
    public void visit(ConnectByRootOperator aThis) {
        fail();
    }

    @Override
    public void visit(OracleNamedFunctionParameter aThis) {
        fail();
    }

    @Override
    public void visit(AllColumns allColumns) {
        fail();
    }

    @Override
    public void visit(AllTableColumns allTableColumns) {
        fail();
    }

    @Override
    public void visit(AllValue allValue) {
        fail();
    }

    @Override
    public void visit(IsDistinctExpression isDistinctExpression) {
        fail();
    }

    @Override
    public void visit(GeometryDistance geometryDistance) {
        fail();
    }

    @Override
    public void visit(Select selectBody) {
        try {
            mNode = SelectProcessor.process(selectBody, mScope);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    @Override
    public void visit(TranscodingFunction transcodingFunction) {
        fail();
    }

    @Override
    public void visit(TrimFunction trimFunction) {
        fail();
    }

    @Override
    public void visit(RangeExpression rangeExpression) {
        fail();
    }

    @Override
    public void visit(TSQLLeftJoin tsqlLeftJoin) {
        fail();
    }

    @Override
    public void visit(TSQLRightJoin tsqlRightJoin) {
        fail();
    }

    private void visitBinaryOp(BinaryExpression expr, int op) {
        expr.getLeftExpression().accept(this);
        Node left = mNode;
        expr.getRightExpression().accept(this);
        Node right = mNode;
        mNode = BinaryOpNode.make(op, left, right);
    }

    private static UnsupportedOperationException fail() {
        throw new UnsupportedOperationException();
    }
}
