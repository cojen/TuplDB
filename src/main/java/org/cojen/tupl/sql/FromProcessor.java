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

import net.sf.jsqlparser.expression.Alias;

import net.sf.jsqlparser.schema.Table;

import net.sf.jsqlparser.statement.select.*;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.model.RelationNode;
import org.cojen.tupl.model.TableNode;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class FromProcessor implements FromItemVisitor {
    /**
     * @param finder used for finding fully functional Table instances
     */
    public static RelationNode process(FromItem item, TableFinder finder) throws IOException {
        var processor = new FromProcessor(finder);
        item.accept(processor);
        return processor.mNode;
    }

    private final TableFinder mFinder;

    private RelationNode mNode;

    private FromProcessor(TableFinder finder) {
        mFinder = finder;
    }

    @Override
    public void visit(Table table) {
        if (table.getPivot() != null ||
            table.getUnPivot() != null ||
            table.getSampleClause() != null)
        {
            throw fail();
        }

        String name = table.getFullyQualifiedName();

        org.cojen.tupl.Table dbTable;

        try {
            dbTable = mFinder.findTable(name);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }

        if (dbTable == null) {
            throw new IllegalStateException("Table not found: " + name);
        }

        Alias alias = table.getAlias();
        if (alias != null) {
            if (alias.getAliasColumns() != null) {
                throw fail();
            }
            name = SqlUtils.unquote(alias.getName());
        }

        mNode = TableNode.make(name, dbTable);
    }

    @Override
    public void visit(ParenthesedSelect selectBody) {
        mNode = SelectProcessor.process(selectBody, mFinder);
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect) {
        fail();
    }

    @Override
    public void visit(TableFunction valuesList) {
        fail();
    }

    @Override
    public void visit(ParenthesedFromItem aThis) {
        fail();
    }

    private static UnsupportedOperationException fail() {
        throw new UnsupportedOperationException();
    }
}
