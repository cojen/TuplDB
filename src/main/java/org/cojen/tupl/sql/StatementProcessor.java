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

import java.io.IOException;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.ParseException;

import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.alter.*;
import net.sf.jsqlparser.statement.alter.sequence.*;
import net.sf.jsqlparser.statement.analyze.*;
import net.sf.jsqlparser.statement.comment.*;
import net.sf.jsqlparser.statement.create.index.*;
import net.sf.jsqlparser.statement.create.schema.*;
import net.sf.jsqlparser.statement.create.sequence.*;
import net.sf.jsqlparser.statement.create.synonym.*;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.create.view.*;
import net.sf.jsqlparser.statement.delete.*;
import net.sf.jsqlparser.statement.drop.*;
import net.sf.jsqlparser.statement.execute.*;
import net.sf.jsqlparser.statement.grant.*;
import net.sf.jsqlparser.statement.insert.*;
import net.sf.jsqlparser.statement.merge.*;
import net.sf.jsqlparser.statement.refresh.*;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.show.*;
import net.sf.jsqlparser.statement.truncate.*;
import net.sf.jsqlparser.statement.update.*;
import net.sf.jsqlparser.statement.upsert.*;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class StatementProcessor implements StatementVisitor {
    public static Object process(String sql, Scope scope)
        throws ParseException, IOException
    {
        CCJSqlParser parser = CCJSqlParserUtil.newParser(sql);
        return process(parser.Statement(), scope);
    }

    public static Object process(Statement statement, Scope scope) throws IOException {
        var processor = new StatementProcessor(scope);
        statement.accept(processor);
        return processor.mStatement;
    }

    private final Scope mScope;

    private Object mStatement;

    private StatementProcessor(Scope scope) {
        mScope = scope;
    }

    @Override
    public void visit(Comment comment) {
        fail();
    }

    @Override
    public void visit(Commit commit) {
        fail();
    }

    @Override
    public void visit(Select select) {
        try {
            mStatement = SelectProcessor.process(select, mScope);
        } catch (IOException e) {
            throw Utils.rethrow(e);
        }
    }

    @Override
    public void visit(Delete delete) {
        fail();
    }

    @Override
    public void visit(Update update) {
        fail();
    }

    @Override
    public void visit(Insert insert) {
        fail();
    }

    @Override
    public void visit(Drop drop) {
        fail();
    }

    @Override
    public void visit(Truncate truncate) {
        fail();
    }

    @Override
    public void visit(CreateIndex createIndex) {
        fail();
    }

    @Override
    public void visit(CreateSchema aThis) {
        fail();
    }

    @Override
    public void visit(CreateTable createTable) {
        fail();
    }

    @Override
    public void visit(CreateView createView) {
        fail();
    }

    @Override
    public void visit(Alter alter) {
        fail();
    }

    @Override
    public void visit(Statements stmts) {
        fail();
    }

    @Override
    public void visit(Execute execute) {
        fail();
    }

    @Override
    public void visit(SetStatement set) {
        fail();
    }

    @Override
    public void visit(ResetStatement reset) {
        fail();
    }

    @Override
    public void visit(Merge merge) {
        fail();
    }

    @Override
    public void visit(AlterView alterView) {
        fail();
    }

    @Override
    public void visit(Upsert upsert) {
        fail();
    }

    @Override
    public void visit(UseStatement use) {
        fail();
    }

    @Override
    public void visit(Block block) {
        fail();
    }

    @Override
    public void visit(DescribeStatement describe) {
        fail();
    }

    @Override
    public void visit(ExplainStatement aThis) {
        fail();
    }

    @Override
    public void visit(ShowStatement aThis) {
        fail();
    }

    @Override
    public void visit(ShowColumnsStatement set) {
        fail();
    }

    @Override
    public void visit(ShowIndexStatement set) {
        fail();
    }

    @Override
    public void visit(ShowTablesStatement showTables) {
        fail();
    }

    @Override
    public void visit(DeclareStatement aThis) {
        fail();
    }

    @Override
    public void visit(Grant grant) {
        fail();
    }

    @Override
    public void visit(CreateSequence createSequence) {
        fail();
    }

    @Override
    public void visit(AlterSequence alterSequence) {
        fail();
    }

    @Override
    public void visit(CreateFunctionalStatement createFunctionalStatement) {
        fail();
    }

    @Override
    public void visit(CreateSynonym createSynonym) {
        fail();
    }

    @Override
    public void visit(Analyze analyze) {
        fail();
    }

    @Override
    public void visit(SavepointStatement savepointStatement) {
        fail();
    }

    @Override
    public void visit(RollbackStatement rollbackStatement) {
        fail();
    }

    @Override
    public void visit(AlterSession alterSession) {
        fail();
    }

    @Override
    public void visit(IfElseStatement ifElseStatement) {
        fail();
    }

    @Override
    public void visit(RenameTableStatement renameTableStatement) {
        fail();
    }

    @Override
    public void visit(PurgeStatement purgeStatement) {
        fail();
    }

    @Override
    public void visit(AlterSystemStatement alterSystemStatement) {
        fail();
    }

    @Override
    public void visit(UnsupportedStatement unsupportedStatement) {
        fail();
    }

    @Override
    public void visit(RefreshMaterializedViewStatement materializedView) {
        fail();
    }

    private static UnsupportedOperationException fail() {
        throw new UnsupportedOperationException();
    }
}
