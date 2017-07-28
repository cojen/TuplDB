/*
 *  Copyright (C) 2011-2017 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl;

import java.io.IOException;

/**
 * TreeCursor which uses an explicit transaction when none is specified, excluding loads.
 *
 * @author Brian S O'Neill
 */
/*P*/
final class TxnTreeCursor extends TreeCursor {
    TxnTreeCursor(TxnTree tree, Transaction txn) {
        super(tree, txn);
    }

    TxnTreeCursor(TxnTree tree) {
        super(tree);
    }

    // Note: Replicated transactions (via redo logging) require undo logging for all
    // operations, because rollback is required when commits are rejected.

    @Override
    protected int storeMode() {
        // Always undo.
        return 1;
    }
}
