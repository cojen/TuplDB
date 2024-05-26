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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    public static RelationExpr parse(String query) throws QueryException {
        return parse((RelationExpr) null, query);
    }

    public static RelationExpr parse(Table<?> from, String query) throws QueryException {
        return parse(TableExpr.make(-1, -1, from), query);
    }

    /**
     * @param from can be null if not selecting from any table at all
     */
    public static RelationExpr parse(RelationExpr from, String query) throws QueryException {
        try {
            return new Parser(0, from, query).parseQueryExpr();
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
        return parse(TableExpr.make(-1, -1, rowType), query);
    }

    /**
     * @param paramDelta amount to add to each parameter number after being parsed
     * @param availableColumns can pass null if all columns are available
     */
    public static RelationExpr parse(int paramDelta, Table<?> fromTable,
                                     Set<String> availableColumns, String query)
        throws QueryException
    {
        RelationExpr from = TableExpr.make(-1, -1, fromTable, availableColumns);
        try {
            return new Parser(paramDelta, from, query).parseQueryExpr();
        } catch (IOException e) {
            // Not expected.
            throw new QueryException(e);
        }
    }

    /**
     * Attempt to parse a query into a QuerySpec, throwing a QueryException if not possible.
     */
    public static QuerySpec parseQuerySpec(Class<?> rowType, String query) throws QueryException {
        return parse(rowType, query).querySpec(rowType);
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
            return new Parser(paramDelta, from, query).parseQueryExpr().querySpec(rowType);
        } catch (IOException e) {
            // Not expected.
            throw new QueryException(e);
        }
    }

    private final int mParamDelta;
    private final RelationExpr mFrom;
    private final Tokenizer mTokenizer;

    private Token mNextToken;

    private int mParamOrdinal;

    private Map<String, AssignExpr> mLocalVars;

    /**
     * @param paramDelta amount to add to each parameter number after being parsed
     * @param from can be null if not selecting from any table at all
     */
    public Parser(int paramDelta, RelationExpr from, String source) {
        this(paramDelta, from, new Tokenizer(source));
    }

    /**
     * @param paramDelta amount to add to each parameter number after being parsed
     * @param from can be null if not selecting from any table at all
     */
    public Parser(int paramDelta, RelationExpr from, Reader in) {
        this(paramDelta, from, new Tokenizer(in));
    }

    /**
     * @param paramDelta amount to add to each parameter number after being parsed
     * @param from can be null if not selecting from any table at all
     */
    private Parser(int paramDelta, RelationExpr from, Tokenizer tokenizer) {
        mParamDelta = paramDelta;
        if (from == null) {
            from = TableExpr.identity();
        }
        mFrom = from;
        mTokenizer = tokenizer;
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
     * Projection = "{" ProjExprs "}"
     */
    private RelationExpr doParseQueryExpr() throws IOException {
        final Token first = peekToken();

        final Expr filter;
        final List<ProjExpr> projection;
        final int endPos;

        if (first.type() != T_LBRACE) {
            filter = parseExpr();
            projection = null;
            endPos = filter.endPos();
        } else {
            consumePeek();
            projection = new ArrayList<>(parseProjExprs().values());
            Token next = nextToken();
            if (next.type() != T_RBRACE) {
                throw new QueryException("Right brace expected", next);
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
            throw new QueryException("Unexpected trailing characters", peek);
        }

        return QueryExpr.make(first.startPos(), endPos, mFrom, filter, projection);
    }

    /*
     * ProjExprs = [ ProjExpr { "," ProjExpr } ]
     */
    private Map<String, ProjExpr> parseProjExprs() throws IOException {
        final Token first = peekToken();

        if (first.type() == T_RBRACE) {
            return Collections.emptyMap();
        }

        Map<String, ProjExpr> map = new LinkedHashMap<>();
        Map<String, ColumnExpr> wildcards = null;
        Set<String> excluded = null;

        while (true) {
            ProjExpr expr = parseProjExpr();

            ColumnExpr wildcard;
            addProjection: if ((wildcard = expr.isWildcard()) != null) {
                if (expr.hasExclude()) {
                    throw new QueryException("Cannot exclude by wildcard", expr);
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
                        throw new QueryException("Projection is already excluded: " + name, expr);
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

                        if (wildcards == null || wildcards.remove(name) == null) {
                            throw new QueryException
                                ("Excluded projection not found: " + unescape(name), expr);
                        }

                        if (!expr.hasOrderBy()) {
                            // Don't exclude the projection if it has an order-by flag.
                            break addProjection;
                        }
                    }
                }

                if (map.putIfAbsent(name, expr) != null) {
                    if (wildcards == null || wildcards.remove(name) == null) {
                        throw new QueryException("Duplicate projection: " + unescape(name), expr);
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

        List<Token> path = parsePath();

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
            Token.Text ident = asIdentifier(path.get(0));
            String name = ident.text(true);
            var assign = AssignExpr.make(startPos, right.endPos(), name, right);
            expr = assign;
            if (mLocalVars == null) {
                mLocalVars = new LinkedHashMap<>();
            }
            if (mLocalVars.putIfAbsent(name, assign) != null) {
                throw new QueryException("Duplicate assignment: " + ident.mText, assign);
            }
        } else access: {
            if (mLocalVars != null && path.size() == 1) {
                Token.Text ident = asIdentifier(path.get(0));
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

    // FIXME: Use peeks to skip ahead to parseExpr, reducing stack depth.

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
                throw new QueryException("Equality operator must be specified as '=='", peek);
            }
        }

        return expr;
    }

    /*
     * RelExpr = AddExpr { RelOp AddExpr }
     * RelOp   = ">=" | "<" | "<=" | ">" | "in"
     */
    private Expr parseRelExpr() throws IOException {
        Expr expr = parseAddExpr();

        loop: while (true) {
            Token peek = peekToken();
            int type = peek.type();
            switch (type) {
            default: break loop;
            case T_GE: case T_LT: case T_LE: case T_GT:
                consumePeek();
                Expr right = parseAddExpr();
                expr = BinaryOpExpr.make(expr.startPos(), right.endPos(), type, expr, right);
                break;
            case T_IN:
                consumePeek();
                right = parseAddExpr();
                expr = InExpr.make(expr.startPos(), right.endPos(), expr, right);
                break;
            }
        }

        return expr;
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
                throw new QueryException("Right paren expected", next);
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
                    throw new QueryException("Argument number must be at least one", next);
                }
                if (ordinal > 100) {
                    // Define an arbitrary upper bound to guard against giant argument arrays
                    // from being allocated later on.
                    throw new QueryException("Argument number is too large", next);
                }
                return ParamExpr.make(t.startPos(), next.endPos(), ordinal + mParamDelta);
            } else {
                switch (next.type()) {
                case T_ARG:
                case T_FALSE: case T_TRUE: case T_NULL:
                case T_IDENTIFIER: case T_STRING:
                case T_INT: case T_LONG: case T_BIGINT:
                case T_FLOAT: case T_DOUBLE: case T_BIGDEC:
                    throw new QueryException("Malformed argument number", next);
                }
                pushbackToken(next);
                return ParamExpr.make(t.startPos(), t.endPos(), ++mParamOrdinal + mParamDelta);
            }
        }

        pushbackToken(t);

        List<Token> path = parsePath();

        if (peekTokenType() == T_LPAREN) {
            consumePeek();
            List<Expr> params = parseExprs();
            Token next = nextToken();
            if (next.type() != T_RPAREN) {
                throw new QueryException("Right paren expected", next);
            }
            // FIXME: CallExpr; need function search; exclude wildcards
            throw new RuntimeException("CallExpr: " + path + ", " + params);
        }

        if (mLocalVars != null && path.size() == 1) {
            Token.Text ident = asIdentifier(path.get(0));
            String name = ident.text(true);
            AssignExpr assign = mLocalVars.get(name);
            if (assign != null) {
                return VarExpr.make(ident.startPos(), ident.endPos(), assign);
            }
        }

        return resolveColumn(path, false);
    }

    private ColumnExpr resolveColumn(List<Token> path, boolean allowWildcards) {
        TupleType rowType = mFrom.rowType();
        Token.Text ident = asIdentifier(path.get(0));
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
        return new QueryException(message + ident.text(false), ident);
    }

    /*
     * Path = Identifier { "." Identifier } [ "." | "*" ]
     */
    private List<Token> parsePath() throws IOException {
        Token first = nextToken();

        if (first.type() != T_IDENTIFIER) {
            String msg = first.type() == T_STAR ? "Wildcard disallowed" : "Identifier expected";
            throw new QueryException(msg, first);
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
                throw new QueryException("Identifier or wildcard expected", t);
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
        throw new QueryException("Wildcard disallowed", token);
    }

    private static String toPath(List<Token> path, boolean escaped) {
        int size = path.size();
        if (size == 1) {
            return toPathText(path.get(0), escaped);
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
     * Exprs = [ Expr { "," Expr } ]
     */
    private List<Expr> parseExprs() throws IOException {
        if (peekTokenType() == T_RPAREN) {
            return Collections.emptyList();
        }

        Expr first = parseExpr();

        if (peekTokenType() != T_COMMA) {
            return List.of(first);
        }

        var list = new ArrayList<Expr>();
        list.add(first);

        while (peekTokenType() == T_COMMA) {
            consumePeek();
            list.add(parseExpr());
        }

        return list;
    }

    private Token peekToken() throws IOException {
        Token t = mNextToken;
        if (t != null) {
            return t;
        }
        return mNextToken = mTokenizer.next();
    }

    private int peekTokenType() throws IOException {
        return peekToken().type();
    }

    private void consumePeek() {
        if (mNextToken == null) {
            throw new AssertionError();
        }
        mNextToken = null;
    }

    private Token nextToken() throws IOException {
        Token t = mNextToken;
        if (t != null) {
            mNextToken = null;
            return t;
        }
        return mTokenizer.next();
    }

    private void pushbackToken(Token t) {
        if (mNextToken != null) {
            throw new AssertionError();
        }
        mNextToken = t;
    }
}
