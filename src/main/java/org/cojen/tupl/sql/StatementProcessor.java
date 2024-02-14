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

import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.ParseException;

import net.sf.jsqlparser.schema.Column;

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

import org.cojen.tupl.Table;

import org.cojen.tupl.model.ColumnNode;
import org.cojen.tupl.model.CommandNode;
import org.cojen.tupl.model.InsertNode;
import org.cojen.tupl.model.Node;
import org.cojen.tupl.model.RelationNode;
import org.cojen.tupl.model.SimpleCommand;
import org.cojen.tupl.model.TableNode;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.table.RowInfo;
import org.cojen.tupl.table.RowInfoBuilder;

import static org.cojen.tupl.table.ColumnInfo.*;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class StatementProcessor implements StatementVisitor {
    public static Node process(String sql, Scope scope)
        throws ParseException, IOException
    {
        CCJSqlParser parser = CCJSqlParserUtil.newParser(sql);
        return process(parser.Statement(), scope);
    }

    public static Node process(Statement statement, Scope scope) throws IOException {
        var processor = new StatementProcessor(scope);
        statement.accept(processor);
        return processor.mStatement;
    }

    private final Scope mScope;

    private Node mStatement;

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
        if (insert.getDuplicateUpdateSets() != null ||
            insert.getSetUpdateSets() != null ||
            insert.getOutputClause() != null ||
            insert.getReturningClause() != null ||
            insert.isModifierIgnore() ||
            insert.getWithItemsList() != null ||
            insert.getConflictTarget() != null ||
            insert.getConflictAction() != null)
        {
            throw fail();
        }

        String name = insert.getTable().getFullyQualifiedName();

        Table table;
        try {
            table = mScope.findTable(name);
        } catch (IOException e) {
            throw Utils.rethrow(e);
        }

        if (table == null) {
            throw new IllegalStateException("Table isn't found: " + name);
        }

        TableNode tableNode = TableNode.make(table);

        ExpressionList<Column> columns = insert.getColumns();
        var columnNodes = new ColumnNode[columns.size()];

        for (int i=0; i<columnNodes.length; i++) {
            columnNodes[i] = findColumnNode(tableNode, columns.get(i));
        }

        Select select = insert.getSelect();

        Node[] valueNodes;
        RelationNode selectNode;

        if (select instanceof Values values) {
            selectNode = null;
            valueNodes = ExpressionProcessor.process(values.getExpressions(), mScope);
            if (valueNodes.length != columnNodes.length) {
                throw new IllegalStateException("Number of values doesn't match number of columns");
            }
        } else {
            valueNodes = null;
            try {
                selectNode = SelectProcessor.process(select, mScope);
            } catch (IOException e) {
                throw Utils.rethrow(e);
            }
        }

        mStatement = InsertNode.make(tableNode, columnNodes, valueNodes, selectNode);
    }

    private static ColumnNode findColumnNode(TableNode tableNode, Column column) {
        if (column.getArrayConstructor() != null || column.getTable() != null) {
            throw fail();
        }
        return tableNode.findColumn(column.getColumnName());
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
        if (createIndex.getTailParameters() != null && !createIndex.getTailParameters().isEmpty()) {
            throw fail();
        }

        Index index = createIndex.getIndex();

        if (index.getUsing() != null || index.getIndexSpec() != null) {
            throw fail();
        }

        boolean unique;

        {
            String type = index.getType();
            if (type == null) {
                unique = false;
            } else if ("unique".equalsIgnoreCase(type)) {
                unique = true;
            } else {
                throw fail();
            }
        }

        var specBuf = new StringBuilder();

        for (Index.ColumnParams col : index.getColumns()) {
            List<String> params = col.getParams();
            if (params == null || params.isEmpty()) {
                specBuf.append('+');
            } else if (params.size() != 1) {
                throw fail();
            } else {
                String param = params.get(0);
                if ("asc".equalsIgnoreCase(param)) {
                    specBuf.append('+');
                } else if ("desc".equalsIgnoreCase(param)) {
                    specBuf.append('-');
                }
            }

            specBuf.append(col.getColumnName());
        }

        String spec = specBuf.toString();

        String tableName = createIndex.getTable().getFullyQualifiedName();
        String indexName = index.getName();
        boolean ifNotExists = createIndex.isUsingIfNotExists();

        SimpleCommand command = (control, txn, args) -> {
            if (txn != null) {
                throw new IllegalArgumentException("Cannot create an index in a transaction");
            }
            mScope.finder().createIndex(control, tableName, indexName, spec, unique, ifNotExists);
            return 0;
        };

        mStatement = CommandNode.make("create", command);
    }

    @Override
    public void visit(CreateSchema aThis) {
        fail();
    }

    @Override
    public void visit(CreateTable createTable) {
        if (createTable.isUnlogged() ||
            createTable.getTableOptionsStrings() != null ||
            createTable.getCreateOptionsStrings() != null ||
            createTable.getIndexes() != null ||
            createTable.getSelect() != null ||
            createTable.getLikeTable() != null ||
            createTable.isOrReplace() ||
            createTable.getRowMovement() != null ||
            createTable.getSpannerInterleaveIn() != null)
        {
            throw fail();
        }

        var b = new RowInfoBuilder(createTable.getTable().getFullyQualifiedName());

        boolean pk = false;
        for (ColumnDefinition def : createTable.getColumnDefinitions()) {
            pk |= addColumn(b, def);
        }

        if (!pk) {
            throw new IllegalStateException("No primary key is defined");
        }

        RowInfo info = b.build();

        boolean ifNotExists = createTable.isIfNotExists();

        SimpleCommand command = (control, txn, args) -> {
            if (txn != null) {
                throw new IllegalArgumentException("Cannot create a table in a transaction");
            }
            mScope.finder().createTable(info, ifNotExists);
            return 0;
        };

        mStatement = CommandNode.make("create", command);
    }

    /**
     * @return true if column was added to the primary key
     */
    private boolean addColumn(RowInfoBuilder b, ColumnDefinition def) {
        ColDataType type = def.getColDataType();

        if (type.getCharacterSet() != null || !type.getArrayData().isEmpty()) {
            throw fail();
        }

        int typeCode = switch (type.getDataType().toUpperCase()) {
            default -> throw new IllegalArgumentException("Unsupported type: " + def);

            case "CHAR", "CHARACTER", "NCHAR", "VARCHAR", "TEXT" ->
                lengthArg(type) == 1 ? TYPE_CHAR : TYPE_UTF8;
            case "BINARY", "VARBINARY" ->  TYPE_UBYTE | TYPE_ARRAY;
            case "BOOL", "BOOLEAN" -> TYPE_BOOLEAN;
            case "TINYINT" -> TYPE_BYTE;
            case "SMALLINT" -> TYPE_SHORT;
            case "INT", "INTEGER" -> TYPE_INT;
            case "BIGINT" -> TYPE_LONG;
            case "NUMERIC", "DECIMAL", "DEC" -> TYPE_BIG_DECIMAL;
            case "REAL", "DOUBLE PRECISION" -> TYPE_DOUBLE;
            case "FLOAT" -> lengthArg(type) <= 24 ? TYPE_FLOAT : TYPE_DOUBLE;
            case "UNSIGNED TINYINT" -> TYPE_UBYTE;
            case "UNSIGNED SMALLINT" -> TYPE_USHORT;
            case "UNSIGNED INT", "UNSIGNED INTEGER" -> TYPE_UINT;
            case "UNSIGNED BIGINT" -> TYPE_ULONG;
        };

        typeCode |= TYPE_NULLABLE;
        boolean pk = false, unique = false;

        List<String> specs = def.getColumnSpecs();

        if (specs != null) for (Iterator<String> it = specs.iterator(); it.hasNext(); ) {
            String spec = it.next();

            if (spec.equalsIgnoreCase("UNSIGNED")) {
                if (typeCode > 0b1111) {
                    throw new IllegalArgumentException("Unsupported type: " + def);
                }
                typeCode &= ~0b1000;
            } else if (spec.equalsIgnoreCase("NOT")) {
                require(def, it, "NULL");
                typeCode &= ~TYPE_NULLABLE;
            } else if (spec.equalsIgnoreCase("PRIMARY")) {
                require(def, it, "KEY");
                pk = true;
            } else if (spec.equalsIgnoreCase("UNIQUE")) {
                unique = true;
            } else {
                throw new IllegalArgumentException("Unsupported type: " + def);
            }
        }

        String name = def.getColumnName();

        b.addColumn(name, typeCode);

        if (pk) {
            b.addToPrimaryKey(name);
        }

        if (unique) {
            b.addToAlternateKey(name);
            b.finishAlternateKey();
        }

        return pk;
    }

    private void require(ColumnDefinition def, Iterator<String> it, String expect) {
        if (!it.hasNext() || !it.next().equalsIgnoreCase(expect)) {
            throw new IllegalArgumentException("Unsupported type: " + def);
        }
    }

    /**
     * @return -1 if unspecified
     */
    private int lengthArg(ColDataType type) {
        List<String> args = type.getArgumentsStringList();
        int size;
        if (args == null || (size = args.size()) == 0) {
            return -1;
        }
        if (size != 1) {
            throw fail();
        }
        return Integer.parseInt(args.get(0));
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
