/*
 *  Copyright (C) 2018 Cojen.org
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

import java.util.concurrent.Executor;

/**
 * Parallel tree merging utility. All entries from the source trees (assumed to be temporary
 * trees) are merged into a new target tree, and all sources are deleted.
 *
 * @author Brian S O'Neill
 */
/*P*/
abstract class TreeMerger extends TreeSeparator {
    /**
     * @param executor used for parallel separation; pass null to use only the starting thread
     * @param workerCount maximum parallelism; must be at least 1
     */
    TreeMerger(LocalDatabase db, Tree[] sources, Executor executor, int workerCount) {
        super(db, sources, executor, workerCount);
    }

    @Override
    protected void finished(Chain<Tree> firstRange) {
        Tree merged = firstRange.element();

        if (merged != null) merge: {
            Chain<Tree> range = firstRange.next();

            while (range != null) {
                Tree tree = range.element();

                if (tree != null) {
                    try {
                        merged = Tree.graftTempTree(merged, tree);
                    } catch (Throwable e) {
                        failed(e);

                        merged(merged);

                        // Pass along ranges that didn't get merged.
                        while (true) {
                            remainder(tree);
                            do {
                                range = range.next();
                                if (range == null) {
                                    break merge;
                                }
                                tree = range.element();
                            } while (tree == null);
                        }
                    }
                }

                range = range.next();
            }

            merged(merged);
        }

        for (Tree source : mSources) {
            if (isEmpty(source)) {
                try {
                    mDatabase.quickDeleteTemporaryTree(source);
                    continue;
                } catch (Throwable e) {
                    failed(e);
                }
            }

            remainder(source);
        }

        remainder(null);
    }

    /**
     * Receives the target tree; called at most once.
     */
    protected abstract void merged(Tree tree);

    /**
     * Receives any remaining source trees when merger is stopped. Is null when all finished.
     */
    protected abstract void remainder(Tree tree);

    private static boolean isEmpty(Tree tree) {
        Node root = tree.mRoot;
        root.acquireShared();
        boolean empty = root.isLeaf() && !root.hasKeys();
        root.releaseShared();

        if (!empty) {
            // Double check with a cursor. Tree might be composed of many empty leaf nodes.
            TreeCursor c = tree.newCursor(Transaction.BOGUS);
            try {
                c.mKeyOnly = true;
                c.first();
                empty = c.key() == null;
            } catch (Throwable e) {
                // Ignore and keep using the tree for now.
            } finally {
                c.reset();
            }
        }

        return empty;
    }
}
