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

import static org.cojen.tupl.table.expr.Token.*;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class Parser {
    /*
      FIXME: testing
    */
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Table<tupl.schema.sakila.customer> table = null;

        if (true) {
            var db = org.cojen.tupl.Database.open(new org.cojen.tupl.DatabaseConfig());

            table = db.openTable(tupl.schema.sakila.customer.class);

            var row = table.newRow();
            row.customer_id(1);
            row.store_id(1);
            row.first_name("John");
            row.last_name("Smith");
            row.email("me@you.com");
            row.address_id(123);
            row.activebool(true);
            row.create_date("2001-01-01");
            row.last_update("2001-11-23");
            row.active(null);
            table.insert(null, row);
        }

        RelationExpr expr = Parser.parse(table, args[0]);
        System.out.println(expr.getClass());
        System.out.println(expr);
        System.out.println("querySpec: " + expr.querySpec(table));

        TableProvider<?> p = expr.makeTableProvider();
        System.out.println("provider: " + p + ", " + p.argumentCount());

        if (p.argumentCount() == 0) {
            Table t = p.table();
            System.out.println("table: " + t);
            System.out.println("rowType: " + t.rowType());

            System.out.println("plan:");
            System.out.println(t.queryAll().scannerPlan(null));

            System.out.println("dump rows...");
            try (var s = t.newScanner(null)) {
                for (Object row = s.row(); row != null; row = s.step(row)) {
                    System.out.println(row);
                    var r = (org.cojen.tupl.Row) row;

                    t.forEach(r, (n, v) -> {
                        System.out.println("---");
                        System.out.println("nv: " + n + " -> " + v);
                        System.out.println("columnMethodName: " + r.columnMethodName(n));
                        System.out.println("columnType: " + r.columnType(n));
                        System.out.println("get: " + r.get(n));
                        System.out.println("get by method name: " + r.get(r.columnMethodName(n)));
                    });

                    r.set("z", 123.0);
                    System.out.println(r);
                }
            }
            System.out.println("...done");
        }
    }

    /*

      FIXME: Notes:

      Can support sub queries with this entity variant: Path Projection [ Filter ]

      The Parser should have a weak cache. The existing filter Parser can go away, and because
      the Parser has a cache, any queries which can be implemented without mapping won't likely
      be double parsed.

     */

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
            return new Parser(from, query).parseQueryExpr();
        } catch (IOException e) {
            // Not expected.
            throw new QueryException(e);
        }
    }

    private final RelationExpr mFrom;
    private final Tokenizer mTokenizer;

    private Token mNextToken;

    private int mParamOrdinal;

    private Map<String, Type> mLocalVars;

    /**
     * @param from can be null if not selecting from any table at all
     */
    public Parser(RelationExpr from, String source) {
        this(from, new Tokenizer(source));
    }

    /**
     * @param from can be null if not selecting from any table at all
     */
    public Parser(RelationExpr from, Reader in) {
        this(from, new Tokenizer(in));
    }

    /**
     * @param from can be null if not selecting from any table at all
     */
    private Parser(RelationExpr from, Tokenizer tokenizer) {
        if (from == null) {
            from = TableExpr.identity();
        }
        mFrom = from;
        mTokenizer = tokenizer;
    }

    public RelationExpr parseQueryExpr() throws IOException, QueryException {
        try {
            return parseQueryExpr(true);
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
    private RelationExpr parseQueryExpr(boolean expectEof) throws IOException {
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
        Set<String> wildcards = null;
        Set<String> excluded = null;

        while (true) {
            Token peek = peekToken();
            ProjExpr expr = parseProjExpr(peek);

            addProjection: if (expr == null) {
                if (wildcards != null) {
                    throw new QueryException("Wildcard can be specified at most once", peek);
                }

                wildcards = new HashSet<String>();
                final int startPos = first.startPos();
                final TupleType rowType = rowType();

                for (Column c : rowType) {
                    String name = c.name();
                    if (!map.containsKey(name)) {
                        wildcards.add(name);
                        if (!c.isHidden()) {
                            map.put(name, ProjExpr.make
                                    (startPos, startPos,
                                     ColumnExpr.make(startPos, startPos, rowType, c), 0));
                        }
                    }
                }
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
                        if (mLocalVars != null && mLocalVars.containsKey(name)) {
                            ProjExpr existing = map.get(name);
                            if (existing != null) {
                                map.put(name, existing.withExclude());
                            }
                        } else if (map.remove(name) == null) {
                            if (wildcards == null || !wildcards.remove(name)) {
                                throw new QueryException
                                    ("Excluded projection not found: " + name, expr);
                            }
                        }
                        break addProjection;
                    }
                }

                if (wildcards != null && wildcards.remove(name)) {
                    map.put(name, expr);
                } else if (map.putIfAbsent(name, expr) != null) {
                    throw new QueryException("Duplicate projection: " + name, expr);
                }
            }

            if (peekTokenType() == T_COMMA) {
                consumePeek();
            } else {
                break;
            }
        }

        return map;
    }

    /*
     * ProjExpr = [ ProjOp ] Identifier [ "=" Expr ]
     *          | "*"
     * ProjOp   = [ "~" ] [ ( "+" | "-" ) [ "!" ] ]
     *
     * @return null if wildcard
     */
    private ProjExpr parseProjExpr(final Token first) throws IOException {
        final int startPos = first.startPos();
        int type = first.type();

        if (type == T_STAR) { // wildcard
            consumePeek();
            return null;
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

        Token.Text ident = parseIdentifier();
        String name = ident.text(true);

        Expr expr;

        type = peekTokenType();

        if (type == T_ASSIGN) {
            consumePeek();
            Expr right = parseExpr();
            expr = AssignExpr.make(startPos, right.endPos(), name, right);
            if (mLocalVars == null) {
                mLocalVars = new LinkedHashMap<>();
            }
            if (mLocalVars.putIfAbsent(name, expr.type()) != null) {
                throw new QueryException("Duplicate assignment: " + ident.mText, expr);
            }
        } else {
            Type varType;
            if (mLocalVars != null && (varType = mLocalVars.get(name)) != null) {
                expr = VarExpr.make(startPos, ident.endPos(), varType, name);
            } else {
                TupleType rowType = rowType();
                Column c = rowType.tryColumnFor(name);
                if (c == null) {
                    throw new QueryException("Unknown column or variable: " + ident.mText,
                                             startPos, ident.endPos());
                }
                expr = ColumnExpr.make(startPos, ident.endPos(), rowType, c);
            }
        }

        return ProjExpr.make(startPos, expr.endPos(), expr, flags);
    }

    /*
     * Expr = OrExpr
     */
    private Expr parseExpr() throws IOException {
        return parseOrExpr();
    }

    /*
     * OrExpr = AndExpr { OrOp AndExpr }
     * OrOp   = "|" | "||"
     */
    private Expr parseOrExpr() throws IOException {
        Expr expr = parseAndExpr();

        while (true) {
            int type = peekTokenType();
            if (type == T_OR || type == T_LOR) {
                consumePeek();
                Expr right = parseAndExpr();
                expr = BinaryOpExpr.make(expr.startPos(), right.endPos(), type, expr, right);
            } else {
                break;
            }
        }
            
        return expr;
    }

    /*
     * AndExpr = RelExpr { AndOp RelExpr }
     * AndOp   = "&" | "&&"
     */
    private Expr parseAndExpr() throws IOException {
        Expr expr = parseRelExpr();

        while (true) {
            int type = peekTokenType();
            if (type == T_AND || type == T_LAND) {
                consumePeek();
                Expr right = parseRelExpr();
                expr = BinaryOpExpr.make(expr.startPos(), right.endPos(), type, expr, right);
            } else {
                break;
            }
        }

        return expr;
    }

    /*
     * RelExpr = AddExpr { RelOp AddExpr }
     * RelOp   = "==" | "!=" | ">=" | "<" | "<=" | ">"
     */
    private Expr parseRelExpr() throws IOException {
        Expr expr = parseAddExpr();

        loop: while (true) {
            Token peek = peekToken();
            int type = peek.type();
            switch (type) {
            default: break loop;
            case T_EQ: case T_NE: case T_GE: case T_LT: case T_LE: case T_GT:
                consumePeek();
                Expr right = parseAddExpr();
                expr = BinaryOpExpr.make(expr.startPos(), right.endPos(), type, expr, right);
                break;
            case T_ASSIGN:
                throw new QueryException("Equality operator must be specified as '=='", peek);
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
                return ParamExpr.make(t.startPos(), next.endPos(), ordinal);
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
                return ParamExpr.make(t.startPos(), t.endPos(), ++mParamOrdinal);
            }
        }

        pushbackToken(t);

        List<Token.Text> idents = parsePath();

        if (peekTokenType() == T_LPAREN) {
            consumePeek();
            List<Expr> params = parseExprs();
            Token next = nextToken();
            if (next.type() != T_RPAREN) {
                throw new QueryException("Right paren expected", next);
            }
            // FIXME: CallExpr; need function search
            throw new RuntimeException("CallExpr: " + idents + ", " + params);
        }

        if (mLocalVars != null && idents.size() == 1) {
            Token.Text ident = idents.get(0);
            String name = ident.text(true);
            Type varType = mLocalVars.get(name);
            if (varType != null) {
                return VarExpr.make(ident.startPos(), ident.endPos(), varType, name);
            }
        }

        int startPos = idents.get(0).startPos();
        int endPos = idents.get(idents.size() - 1).endPos();

        TupleType rowType = rowType();
        Column c = rowType.tryFindColumn(toPath(idents, true));

        if (c == null) {
            throw new QueryException
                ("Unknown column or variable: " + toPath(idents, false), startPos, endPos);
        }

        return ColumnExpr.make(startPos, endPos, rowType, c);
    }

    /*
     * Path = Identifier { "." Identifier }
     */
    private List<Token.Text> parsePath() throws IOException {
        Token.Text first = parseIdentifier();

        if (peekTokenType() != T_DOT) {
            return List.of(first);
        }

        var list = new ArrayList<Token.Text>();
        list.add(first);

        while (peekTokenType() == T_DOT) {
            consumePeek();
            list.add(parseIdentifier());
        }

        return list;
    }

    private static String toPath(List<Token.Text> idents, boolean escaped) {
        int size = idents.size();
        if (size == 1) {
            return idents.get(0).text(escaped);
        }
        var b = new StringBuilder();
        for (int i=0; i<size; i++) {
            if (i > 0) {
                b.append('.');
            }
            b.append(idents.get(i).text(escaped));
        }
        return b.toString();
    }

    private Token.Text parseIdentifier() throws IOException {
        Token t = nextToken();
        if (t.type() != T_IDENTIFIER) {
            throw new QueryException("Identifier expected", t);
        }
        return (Token.Text) t;
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

    private int nextTokenType() throws IOException {
        return nextToken().type();
    }

    private void pushbackToken(Token t) {
        if (mNextToken != null) {
            throw new AssertionError();
        }
        mNextToken = t;
    }

    private TupleType rowType() {
        return mFrom.type().rowType();
    }
}
