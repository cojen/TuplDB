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

package org.cojen.tupl.table.expr;

import java.io.IOException;
import java.io.Reader;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.cojen.tupl.QueryException;
import org.cojen.tupl.Table;

import org.cojen.tupl.table.filter.QuerySpec;

import static org.cojen.tupl.table.RowMethodsMaker.unescape;

import static org.cojen.tupl.table.expr.Token.*;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class Parser {
    // FIXME: Consider caching the results.

    public static RelationExpr parse(String query) throws QueryException {
        return parse((RelationExpr) null, null, query);
    }

    /**
     * @param rowType the row type class to use, or else null to select one automatically
     */
    public static RelationExpr parse(Table<?> from, Class<?> rowType, String query)
        throws QueryException
    {
        return parse(TableExpr.make(-1, -1, from), rowType, query);
    }

    /**
     * @param from can be null if not selecting from any table at all
     * @param rowType the row type class to use, or else null to select one automatically
     */
    public static RelationExpr parse(RelationExpr from, Class<?> rowType, String query)
        throws QueryException
    {
        try {
            return new Parser(0, from, rowType, query).parseQueryExpr();
        } catch (IOException e) {
            // Not expected.
            throw new QueryException(e);
        }
    }

    /**
     * Parse a RelationExpr which cannot produce CompiledQuery instances. Attempting to do so
     * causes an IllegalStateException to be thrown.
     */
    public static RelationExpr parse(Class<?> rowType, String query) throws QueryException {
        return parse(TableExpr.make(-1, -1, rowType), rowType, query);
    }

    /**
     * @param paramDelta amount to add to each parameter number after being parsed
     * @param rowType the row type class to use, or else null to select one automatically
     * @param availableColumns can pass null if all columns are available
     */
    public static RelationExpr parse(int paramDelta, Table<?> fromTable, Class<?> rowType,
                                     Set<String> availableColumns, String query)
        throws QueryException
    {
        RelationExpr from = TableExpr.make(-1, -1, fromTable, availableColumns);
        try {
            return new Parser(paramDelta, from, rowType, query).parseQueryExpr();
        } catch (IOException e) {
            // Not expected.
            throw new QueryException(e);
        }
    }

    /**
     * Attempt to parse a query into a QuerySpec, throwing a QueryException if not possible.
     */
    public static QuerySpec parseQuerySpec(Class<?> rowType, String query) throws QueryException {
        return parse(rowType, query).querySpec();
    }

    /**
     * Attempt to parse a query into a QuerySpec, throwing a QueryException if not possible.
     *
     * @param paramDelta amount to add to each parameter number after being parsed
     * @param availableColumns can pass null if all columns are available
     */
    public static QuerySpec parseQuerySpec(int paramDelta, Class<?> rowType,
                                           Set<String> availableColumns, String query)
        throws QueryException
    {
        RelationExpr from = TableExpr.make(-1, -1, rowType, availableColumns);
        try {
            return new Parser(paramDelta, from, rowType, query).parseQueryExpr().querySpec();
        } catch (IOException e) {
            // Not expected.
            throw new QueryException(e);
        }
    }

    private final int mParamDelta;
    private final RelationExpr mFrom;
    private final Class<?> mRowType;
    private final Tokenizer mTokenizer;
    private final ArrayDeque<Token> mTokenStack;

    private int mParamOrdinal;

    private Map<String, AssignExpr> mLocalVars;

    private Map<String, ProjExpr> mProjectionMap;

    /**
     * @param paramDelta amount to add to each parameter number after being parsed
     * @param from can be null if not selecting from any table at all
     * @param rowType the row type class to use, or else null to select one automatically
     */
    public Parser(int paramDelta, RelationExpr from, Class<?> rowType, String source) {
        this(paramDelta, from, rowType, new Tokenizer(source));
    }

    /**
     * @param paramDelta amount to add to each parameter number after being parsed
     * @param from can be null if not selecting from any table at all
     * @param rowType the row type class to use, or else null to select one automatically
     */
    public Parser(int paramDelta, RelationExpr from, Class<?> rowType, Reader in) {
        this(paramDelta, from, rowType, new Tokenizer(in));
    }

    /**
     * @param paramDelta amount to add to each parameter number after being parsed
     * @param from can be null if not selecting from any table at all
     * @param rowType the row type class to use, or else null to select one automatically
     */
    private Parser(int paramDelta, RelationExpr from, Class<?> rowType, Tokenizer tokenizer) {
        mParamDelta = paramDelta;
        if (from == null) {
            from = TableExpr.joinIdentity();
        }
        mFrom = from;
        mRowType = rowType;
        mTokenizer = tokenizer;
        mTokenStack = new ArrayDeque<Token>(2);
    }

    public RelationExpr parseQueryExpr() throws IOException, QueryException {
        try {
            return doParseQueryExpr();
        } catch (QueryException e) {
            // FIXME: Refine exceptions to include the source position and context. Also prune
            // a deep stack trace?
            throw e;
        }
    }

    /*
     * QueryExpr  = Projection [ Filter ]
     *            | Filter
     * Filter     = Expr
     * Projection = "{" ProjExprs [ ";" ProjExprs ] "}"
     */
    private RelationExpr doParseQueryExpr() throws IOException {
        final Token first = peekToken();

        final Expr filter;
        final List<ProjExpr> projection;
        final int groupBy;
        final int endPos;

        if (first.type() != T_LBRACE) {
            filter = parseExpr();
            projection = null;
            groupBy = -1;
            endPos = filter.endPos();
        } else {
            consumePeek();
            Map<String, ProjExpr> projExprs = parseProjExprs();
            Token next = nextToken();
            if (next.type() != T_SEMI) {
                projection = new ArrayList<>(projExprs.values());
                groupBy = -1;
            } else {
                groupBy = projExprs.size();
                projection = prepareGroupBy(projExprs, parseProjExprs());
                next = nextToken();
            }
            if (next.type() != T_RBRACE) {
                String message = next.type() == T_SEMI
                    ? "At most one group specification is allowed" : "Right brace expected";
                throw next.queryException(message);
            }
            if (peekTokenType() == T_EOF) {
                filter = null;
                endPos = next.endPos();
            } else {
                filter = parseExpr();
                endPos = filter.endPos();
            }
        }

        Token peek = peekToken();
        if (peek.type() != T_EOF) {
            throw peek.queryException("Unexpected trailing characters");
        }

        if (groupBy < 0) {
            if (projection != null) {
                projection.forEach(Parser::verifyNoGrouping);
            }
            if (filter != null) {
                verifyNoGrouping(filter);
            }
        }

        return QueryExpr.make(first.startPos(), endPos, mFrom, mRowType,
                              filter, projection, groupBy);
    }

    private static void verifyNoGrouping(Expr expr) {
        if (expr.isGrouping()) {
            throw expr.queryException("Query depends on a function which requires grouping, " +
                                      "but no group specification is defined");
        }
    }

    /**
     * @return combined projection
     */
    private static List<ProjExpr> prepareGroupBy(Map<String, ProjExpr> groupByExprs,
                                                 Map<String, ProjExpr> projExprs)
    {
        for (Map.Entry<String, ProjExpr> e : projExprs.entrySet()) {
            String name = e.getKey();
            ProjExpr expr = e.getValue();
            if (groupByExprs.containsKey(name)) {
                throw expr.queryException("Duplicate projection: " + unescape(name));
            }
        }

        for (ProjExpr expr : groupByExprs.values()) {
            if (expr.isGrouping()) {
                throw expr.queryException("Group specification depends on a function " +
                                         "which itself needs a group specification");
            }
        }

        var projection = new ArrayList<ProjExpr>(groupByExprs.size() + projExprs.size());
        projection.addAll(groupByExprs.values());
        projection.addAll(projExprs.values());

        return projection;
    }

    /*
     * ProjExprs = [ ProjExpr { "," ProjExpr } ]
     */
    private Map<String, ProjExpr> parseProjExprs() throws IOException {
        final Token first = peekToken();

        if (first.type() == T_RBRACE || first.type() == T_SEMI) {
            return Map.of();
        }

        if (mProjectionMap != null) {
            // A ProjExpr cannot directly or indirectly reference another ProjExpr. If this
            // changes, some sort of stack would be needed to manage the nesting.
            throw new AssertionError();
        }

        Map<String, ProjExpr> map = new LinkedHashMap<>();
        Map<String, ColumnExpr> wildcards = null;
        Set<String> excluded = null;

        // Used when making CallExpr instances.
        mProjectionMap = map;

        while (true) {
            ProjExpr expr = parseProjExpr();

            ColumnExpr wildcard;
            addProjection: if ((wildcard = expr.isWildcard()) != null) {
                if (expr.hasExclude()) {
                    throw expr.queryException("Cannot exclude by wildcard");
                }
                if (wildcards == null) {
                    wildcards = new LinkedHashMap<>();
                }
                wildcard.expandWildcard(wildcards);
            } else {
                String name = expr.name();

                if (expr.hasExclude()) {
                    if (excluded == null) {
                        excluded = new HashSet<>();
                    }
                    if (!excluded.add(name)) {
                        throw expr.queryException("Projection is already excluded: " + name);
                    }

                    if (!(expr.wrapped() instanceof AssignExpr)) {
                        ProjExpr existing = map.get(name);

                        if (existing != null) {
                            if (existing.hasOrderBy() ||
                                (mLocalVars != null && mLocalVars.containsKey(name)))
                            {
                                // Don't remove the projection if it has an order-by flag or if
                                // it's a local variable. Instead, just set the exclude flag.
                                // Note that if the exclude specified a different order-by, it
                                // can be safely ignored because it would have no effect.
                                expr = existing.withExclude();
                                map.put(name, expr);
                            } else {
                                map.remove(name);
                            }

                            break addProjection;
                        }

                        if (wildcards != null) {
                            wildcards.remove(name);
                        }

                        if (!expr.hasOrderBy()) {
                            // Don't exclude the projection if it has an order-by flag.
                            break addProjection;
                        }
                    }
                }

                if (map.putIfAbsent(name, expr) != null) {
                    if (wildcards == null || wildcards.remove(name) == null) {
                        throw expr.queryException("Duplicate projection: " + unescape(name));
                    }
                }
            }

            if (peekTokenType() == T_COMMA) {
                consumePeek();
            } else {
                break;
            }
        }

        if (wildcards != null) {
            // Project all the remaining wildcard columns at the end, preserving the ordering
            // of columns which were explicitly specified.

            final int pos = first.startPos();

            wildcards.forEach((String name, ColumnExpr ce) -> {
                map.computeIfAbsent(name, n -> ProjExpr.make(pos, pos, ce, 0));
            });
        }

        mProjectionMap = null;

        return map;
    }

    /*
     * ProjExpr = [ ProjOp ] Identifier [ "=" Expr ]
     *          | [ ProjOp ] Path
     *          | "*"
     * ProjOp   = [ "~" ] [ ( "+" | "-" ) [ "!" ] ]
     */
    private ProjExpr parseProjExpr() throws IOException {
        final Token first = peekToken();
        final int startPos = first.startPos();
        int type = first.type();

        if (type == T_STAR) { // wildcard
            consumePeek();
            return ProjExpr.make(startPos, startPos,
                                 ColumnExpr.make(startPos, startPos, mFrom.rowType(), null), 0);
        }

        int flags = 0;

        if (type == T_TILDE) { // exclude
            consumePeek();
            flags |= ProjExpr.F_EXCLUDE;
            type = peekTokenType();
        }

        if (type == T_PLUS || type == T_MINUS) { // order-by
            consumePeek();
            flags |= ProjExpr.F_ORDER_BY;
            if (type == T_MINUS) {
                flags |= ProjExpr.F_DESCENDING;
            }
            type = peekTokenType();
            if (type == T_NOT) { // null low
                consumePeek();
                flags |= ProjExpr.F_NULL_LOW;
            }
        }

        List<Token> path = parsePath(true);

        final Expr expr;

        Token peek = peekToken();
        type = peek.type();

        if (type == T_ASSIGN) {
            if (path.size() != 1) {
                throw new QueryException("Cannot assign to a path: " + toPath(path, false),
                                         startPos, peek.endPos());
            }
            consumePeek();
            Expr right = parseExpr();
            Token.Text ident = asIdentifier(path.getFirst());
            String name = ident.text(true);
            var assign = AssignExpr.make(startPos, right.endPos(), name, right);
            expr = assign;
            if (mLocalVars == null) {
                mLocalVars = new LinkedHashMap<>();
            }
            if (mLocalVars.putIfAbsent(name, assign) != null) {
                throw assign.queryException("Duplicate assignment: " + ident.mText);
            }
        } else access: {
            if (mLocalVars != null && path.size() == 1) {
                Token.Text ident = asIdentifier(path.getFirst());
                String name = ident.text(true);
                AssignExpr assign = mLocalVars.get(name);
                if (assign != null) {
                    expr = VarExpr.make(startPos, ident.endPos(), assign);
                    break access;
                }
            }

            expr = resolveColumn(path, true);
        }

        return ProjExpr.make(startPos, expr.endPos(), expr, flags);
    }

    // TODO: Use peeks to skip ahead to parseExpr, reducing stack depth.

    /*
     * Expr = LogicalOrExpr
     */
    private Expr parseExpr() throws IOException {
        return parseLogicalOrExpr();
    }

    /*
     * LogicalOrExpr = LogicalAndExpr { "||" LogicalAndExpr }
     */
    private Expr parseLogicalOrExpr() throws IOException {
        Expr expr = parseLogicalAndExpr();

        while (true) {
            int type = peekTokenType();
            if (type == T_LOR) {
                consumePeek();
                Expr right = parseLogicalAndExpr();
                expr = BinaryOpExpr.make(expr.startPos(), right.endPos(), type, expr, right);
            } else {
                break;
            }
        }
            
        return expr;
    }

    /*
     * LogicalAndExpr = BitwiseOrExpr { "&&" BitwiseOrExpr }
     */
    private Expr parseLogicalAndExpr() throws IOException {
        Expr expr = parseBitwiseOrExpr();

        while (true) {
            int type = peekTokenType();
            if (type == T_LAND) {
                consumePeek();
                Expr right = parseBitwiseOrExpr();
                expr = BinaryOpExpr.make(expr.startPos(), right.endPos(), type, expr, right);
            } else {
                break;
            }
        }

        return expr;
    }

    /*
     * BitwiseOrExpr = BitwiseXorExpr { "|" BitwiseXorExpr }
     */
    private Expr parseBitwiseOrExpr() throws IOException {
        Expr expr = parseBitwiseXorExpr();

        while (true) {
            int type = peekTokenType();
            if (type == T_OR) {
                consumePeek();
                Expr right = parseBitwiseXorExpr();
                expr = BinaryOpExpr.make(expr.startPos(), right.endPos(), type, expr, right);
            } else {
                break;
            }
        }

        return expr;
    }

    /*
     * BitwiseXorExpr = BitwiseAndExpr { "^" BitwiseAndExpr }
     */
    private Expr parseBitwiseXorExpr() throws IOException {
        Expr expr = parseBitwiseAndExpr();

        while (true) {
            int type = peekTokenType();
            if (type == T_XOR) {
                consumePeek();
                Expr right = parseBitwiseAndExpr();
                expr = BinaryOpExpr.make(expr.startPos(), right.endPos(), type, expr, right);
            } else {
                break;
            }
        }

        return expr;
    }

    /*
     * BitwiseAndExpr = EqualityExpr { "&" EqualityExpr }
     */
    private Expr parseBitwiseAndExpr() throws IOException {
        Expr expr = parseEqualityExpr();

        while (true) {
            int type = peekTokenType();
            if (type == T_AND) {
                consumePeek();
                Expr right = parseEqualityExpr();
                expr = BinaryOpExpr.make(expr.startPos(), right.endPos(), type, expr, right);
            } else {
                break;
            }
        }

        return expr;
    }

    /*
     * EqualityExpr = RelExpr { EqualityOp RelExpr }
     * EqualityOp   = "==" | "!="
     */
    private Expr parseEqualityExpr() throws IOException {
        Expr expr = parseRelExpr();

        loop: while (true) {
            Token peek = peekToken();
            int type = peek.type();
            switch (type) {
            default: break loop;
            case T_EQ: case T_NE:
                consumePeek();
                Expr right = parseRelExpr();
                expr = BinaryOpExpr.make(expr.startPos(), right.endPos(), type, expr, right);
                break;
            case T_ASSIGN:
                throw peek.queryException("Equality operator must be specified as '=='");
            }
        }

        return expr;
    }

    /*
     * RelExpr = RangeExpr { RelOp RangeExpr }
     * RelOp   = ">=" | "<" | "<=" | ">" | "in"
     */
    private Expr parseRelExpr() throws IOException {
        Expr expr = parseRangeExpr();

        loop: while (true) {
            Token peek = peekToken();
            int type = peek.type();
            switch (type) {
            default: break loop;
            case T_GE: case T_LT: case T_LE: case T_GT:
                consumePeek();
                Expr right = parseRangeExpr();
                expr = BinaryOpExpr.make(expr.startPos(), right.endPos(), type, expr, right);
                break;
            case T_IN:
                consumePeek();
                right = parseRangeExpr();
                expr = InExpr.make(expr.startPos(), right.endPos(), expr, right);
                break;
            }
        }

        return expr;
    }

    /*
     * RangeExpr = ".." [ AddExpr ]
     *           | AddExpr [ ".." [ AddExpr ] ]
     */
    private Expr parseRangeExpr() throws IOException {
        Token peek;
        if ((peek = peekToken()).type() == T_DOTDOT) {
            consumePeek();
            if (isAddExprStart(peekToken())) {
                // ..end
                Expr end = parseAddExpr();
                return RangeExpr.make(peek.startPos(), end.endPos(), null, end);
            } else {
                // ..
                return RangeExpr.make(peek.startPos(), peek.endPos(), null, null);
            }
        }

        Expr expr = parseAddExpr();

        if ((peek = peekToken()).type() == T_DOTDOT) {
            consumePeek();
            if (isAddExprStart(peekToken())) {
                // start..end
                Expr end = parseAddExpr();
                return RangeExpr.make(expr.startPos(), end.endPos(), expr, end);
            } else {
                // start..
                return RangeExpr.make(expr.startPos(), peek.endPos(), expr, null);
            }
        }

        return expr;
    }

    private static boolean isAddExprStart(Token t) {
        return switch (t.type()) {
            case T_NOT, T_PLUS, T_MINUS, T_LPAREN, T_ARG, 
                T_FALSE, T_TRUE, T_NULL, 
                T_IDENTIFIER, T_STRING,
                T_INT, T_LONG, T_BIGINT, T_FLOAT, T_DOUBLE, T_BIGDEC
                -> true;
            default -> false;
        };
    }

    /*
     * AddExpr = MultExpr { AddOp MultExpr }
     * AddOp   = "+" | "-"
     */
    private Expr parseAddExpr() throws IOException {
        Expr expr = parseMultExpr();

        while (true) {
            int type = peekTokenType();
            if (type == T_PLUS || type == T_MINUS) {
                consumePeek();
                Expr right = parseMultExpr();
                expr = BinaryOpExpr.make(expr.startPos(), right.endPos(), type, expr, right);
            } else {
                break;
            }
        }

        return expr;
    }

    /*
     * MultExpr = UnaryExpr { MultOp UnaryExpr }
     * MultOp   = "*" | "/" | "%"
     */
    private Expr parseMultExpr() throws IOException {
        Expr expr = parseUnaryExpr();

        loop: while (true) {
            int type = peekTokenType();
            switch (type) {
            default: break loop;
            case T_STAR: case T_DIV: case T_REM:
                consumePeek();
                Expr right = parseUnaryExpr();
                expr = BinaryOpExpr.make(expr.startPos(), right.endPos(), type, expr, right);
            }
        }

        return expr;
    }

    /*
     * UnaryExpr = [ UnaryOp ] EntityExpr
     * UnaryOp   = "!" | "+" | "-"
     */
    private Expr parseUnaryExpr() throws IOException {
        Token peek = peekToken();

        switch (peek.type()) {
        default:
            return parseEntityExpr();
        case T_NOT: case T_PLUS: case T_MINUS:
            consumePeek();
            break;
        }

        Expr expr = parseEntityExpr();

        switch (peek.type()) {
        case T_NOT:
            expr = expr.not(peek.startPos());
            break;
        case T_PLUS:
            // Drop it.
            break;
        case T_MINUS:
            expr = expr.negate(peek.startPos(), true);
            break;
        }

        return expr;
    }

    /*
     * EntityExpr = "(" Expr ")"
     *            | Literal
     *            | ArgRef
     *            | Path
     *            | CallExpr
     *
     * ArgRef     = "?" [ uint ]
     *
     * CallExpr   = Path "(" Exprs ")"
     */
    private Expr parseEntityExpr() throws IOException {
        final Token t = nextToken();

        switch (t.type()) {

        case T_LPAREN: {
            Expr expr = parseExpr();
            Token next = nextToken();
            if (next.type() != T_RPAREN) {
                throw next.queryException("Right paren expected");
            }
            return expr;
        }

            // Literal
        case T_FALSE:
            return ConstantExpr.make(t.startPos(), t.endPos(), false);
        case T_TRUE:
            return ConstantExpr.make(t.startPos(), t.endPos(), true);
        case T_NULL:
            return ConstantExpr.makeNull(t.startPos(), t.endPos());
        case T_STRING:
            return ConstantExpr.make(t.startPos(), t.endPos(), ((Token.Text) t).mText);
        case T_INT:
            return ConstantExpr.make(t.startPos(), t.endPos(), ((Token.Int) t).mValue);
        case T_LONG:
            return ConstantExpr.make(t.startPos(), t.endPos(), ((Token.Long) t).mValue);
        case T_BIGINT:
            return ConstantExpr.make(t.startPos(), t.endPos(), ((Token.BigInt) t).mValue);
        case T_FLOAT:
            return ConstantExpr.make(t.startPos(), t.endPos(), ((Token.Float) t).mValue);
        case T_DOUBLE:
            return ConstantExpr.make(t.startPos(), t.endPos(), ((Token.Double) t).mValue);
        case T_BIGDEC:
            return ConstantExpr.make(t.startPos(), t.endPos(), ((Token.BigDec) t).mValue);

            // ArgRef
        case T_ARG:
            Token next = nextToken();
            if (t.endPos() == next.startPos() && next instanceof Token.Int ti) {
                int ordinal = ti.mValue;
                if (ordinal <= 0) {
                    throw next.queryException("Argument number must be at least one");
                }
                if (ordinal > 100) {
                    // Define an arbitrary upper bound to guard against giant argument arrays
                    // from being allocated later on.
                    throw next.queryException("Argument number is too large");
                }
                return ParamExpr.make(t.startPos(), next.endPos(), ordinal + mParamDelta);
            } else {
                switch (next.type()) {
                case T_ARG:
                case T_FALSE: case T_TRUE: case T_NULL:
                case T_IDENTIFIER: case T_STRING:
                case T_INT: case T_LONG: case T_BIGINT:
                case T_FLOAT: case T_DOUBLE: case T_BIGDEC:
                    throw next.queryException("Malformed argument number");
                }
                pushbackToken(next);
                return ParamExpr.make(t.startPos(), t.endPos(), ++mParamOrdinal + mParamDelta);
            }
        }

        pushbackToken(t);

        List<Token> path = parsePath(false);

        if (peekTokenType() == T_LPAREN) {
            return parseCallExpr(path);
        }

        if (mLocalVars != null && path.size() == 1) {
            Token.Text ident = asIdentifier(path.getFirst());
            String name = ident.text(true);
            AssignExpr assign = mLocalVars.get(name);
            if (assign != null) {
                return VarExpr.make(ident.startPos(), ident.endPos(), assign);
            }
        }

        return resolveColumn(path, false);
    }

    /**
     * Note: A peek token must exist, which is immediately consumed.
     */
    private CallExpr parseCallExpr(List<Token> path) throws IOException {
        consumePeek();

        Map<String, Expr> namedArgs = new LinkedHashMap<>();
        List<Expr> args = parseCallArgs(namedArgs);
        if (namedArgs.isEmpty()) {
            namedArgs = Map.of();
        }

        Token next = nextToken();
        if (next.type() != T_RPAREN) {
            throw next.queryException("Right paren expected");
        }

        int startPos = path.getFirst().startPos();
        int endPos = next.endPos();

        String name = toPath(path, false);

        // FIXME: argTypes, namedArgTypes, reason
        FunctionApplier applier = StandardFunctionFinder.THE
            .tryFindFunction(name, null, null, null);

        if (applier == null) {
            // FIXME: reason
            throw new QueryException("Unknown function: " + name, startPos, endPos);
        }

        return CallExpr.make(startPos, endPos, name, args, namedArgs, applier, mProjectionMap);
    }

    private ColumnExpr resolveColumn(List<Token> path, boolean allowWildcards) throws IOException {
        if (peekTokenType() == T_LPAREN) {
            // A function call isn't allowed here, but parse it anyhow to provide a better
            // error message.
            CallExpr call = parseCallExpr(path);
            throw call.queryException("Expression result must be assigned to a column");
        }

        TupleType rowType = mFrom.rowType();
        Token.Text ident = asIdentifier(path.getFirst());
        Column c = rowType.tryFindColumn(ident.text(true));

        if (c == null) {
            throw unknown("Unknown column or variable: ", ident);
        }

        ColumnExpr ce = ColumnExpr.make(ident.startPos(), ident.endPos(), rowType, c);

        for (int i=1; i<path.size(); i++) {
            Token element = path.get(i);
            if (!(element instanceof Token.Text) && allowWildcards) {
                // Make a wildcard ColumnExpr.
                int startPos = element.startPos();
                int endPos = element.endPos();
                if (ce.type() instanceof TupleType) {
                    ce = ColumnExpr.make(startPos, endPos, ce, null);
                } else {
                    throw new QueryException
                        ("Wildcard disallowed for scalar column", startPos, endPos);
                }
            } else {
                ident = asIdentifier(path.get(i));
                Type type = ce.type();
                if (!(type instanceof TupleType te) ||
                    (c = te.tryFindColumn(ident.text(true))) == null)
                {
                    throw unknown("Unknown sub column: ", ident);
                }
                ce = ColumnExpr.make(ident.startPos(), ident.endPos(), ce, c);
            }
        }

        return ce;
    }

    private static QueryException unknown(String message, Token.Text ident) {
        return ident.queryException(message + ident.text(false));
    }

    /*
     * Path = Identifier { "." Identifier } [ "." | "*" ]
     *
     * @param exprCheck when true, check if an erroneous path is actually a misplaced expression
     */
    private List<Token> parsePath(boolean exprCheck) throws IOException {
        Token first = nextToken();

        if (first.type() != T_IDENTIFIER) {
            String message;
            if (first.type() == T_STAR) {
                message = "Wildcard disallowed";
            } else if (exprCheck) {
                // An expression isn't allowed here, but parse it anyhow to provide a better
                // error message.
                pushbackToken(first);
                Expr expr = parseExpr();
                throw expr.queryException("Expression result must be assigned to a column");
            } else {
                message = "Identifier expected";
            }

            throw first.queryException(message);
        }

        if (peekTokenType() != T_DOT) {
            return List.of(first);
        }

        var list = new ArrayList<Token>();
        list.add(first);

        while (peekTokenType() == T_DOT) {
            consumePeek();

            Token t = nextToken();
            if (t.type() == T_IDENTIFIER) {
                list.add(t);
            } else if (t.type() == T_STAR) {
                list.add(t);
                break;
            } else {
                throw t.queryException("Identifier or wildcard expected");
            }
        }

        return list;
    }

    /**
     * @param token expected to be Token.Text or wildcard
     * @throws QueryException if not Token.Text
     */
    private static Token.Text asIdentifier(Token token) {
        if (token instanceof Token.Text text) {
            return text;
        }
        throw token.queryException("Wildcard disallowed");
    }

    private static String toPath(List<Token> path, boolean escaped) {
        int size = path.size();
        if (size == 1) {
            return toPathText(path.getFirst(), escaped);
        }
        var b = new StringBuilder();
        for (int i=0; i<size; i++) {
            if (i > 0) {
                b.append('.');
            }
            b.append(toPathText(path.get(i), escaped));
        }
        return b.toString();
    }

    /**
     * @param token expected to be Token.Text or wildcard
     */
    private static String toPathText(Token token, boolean escaped) {
        return token instanceof Token.Text text ? text.text(escaped) : "*";
    }

    /*
     * CallArgs = [ CallArg { "," CallArg } ]
     */
    private List<Expr> parseCallArgs(Map<String, Expr> namedArgs) throws IOException {
        if (peekTokenType() == T_RPAREN) {
            return List.of();
        }

        Expr first = parseCallArg(namedArgs);

        if (peekTokenType() != T_COMMA && first == null) {
            return List.of();
        }

        var list = new ArrayList<Expr>();

        if (first != null) {
            list.add(first);
        }

        while (peekTokenType() == T_COMMA) {
            consumePeek();
            Expr expr = parseCallArg(namedArgs);
            if (expr != null) {
                list.add(expr);
            }
        }

        return list;
    }

    /*
     * CallArg = [ Identifier ':' ] Expr
     *
     * @return null if a named arg was parsed instead
     */
    private Expr parseCallArg(Map<String, Expr> namedArgs) throws IOException {
        {
            Token token = nextToken();

            if (token.type() == T_IDENTIFIER && peekTokenType() == T_COLON) {
                consumePeek();
                String name = ((Token.Text) token).mText;
                if (namedArgs.putIfAbsent(name, parseExpr()) == null) {
                    return null;
                }
                throw token.queryException("Duplicate named argument: " + unescape(name));
            }

            pushbackToken(token);
        }

        return parseExpr();
    }

    private Token peekToken() throws IOException {
        Token t = mTokenStack.peekFirst();
        if (t == null) {
            t = mTokenizer.next();
            mTokenStack.addFirst(t);
        }
        return t;
    }

    private int peekTokenType() throws IOException {
        return peekToken().type();
    }

    private void consumePeek() {
        mTokenStack.removeFirst();
    }

    private Token nextToken() throws IOException {
        Token t = mTokenStack.pollFirst();
        if (t != null) {
            return t;
        }
        return mTokenizer.next();
    }

    private void pushbackToken(Token t) {
        mTokenStack.addFirst(t);
    }
}
