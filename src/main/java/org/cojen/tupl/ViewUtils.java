/*
 *  Copyright 2015 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl;

import java.io.IOException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ViewUtils {
    static long count(View view, boolean autoload, byte[] lowKey, byte[] highKey)
        throws IOException
    {
        long count = 0;
            
        Cursor c = view.newCursor(Transaction.BOGUS);
        try {
            c.autoload(autoload);

            if (lowKey == null) {
                c.first();
            } else {
                c.findGe(lowKey);
            }

            if (highKey == null) {
                for (; c.key() != null; c.next()) {
                    count++;
                }
            } else {
                for (; c.key() != null && c.compareKeyTo(highKey) < 0; c.next()) {
                    count++;
                }
            }
        } finally {
            c.reset();
        }

        return count;
    }

    static byte[] appendZero(byte[] key) {
        byte[] newKey = new byte[key.length + 1];
        System.arraycopy(key, 0, newKey, 0, key.length);
        return newKey;
    }


    static View checkOrdering(View view) {
        Ordering ordering = view.getOrdering();
        if (ordering == Ordering.DESCENDING) {
            view = view.viewReverse();
            ordering = view.getOrdering();
        }
        if (ordering != Ordering.ASCENDING) {
            throw new UnsupportedOperationException("Unsupported ordering: " + ordering);
        }
        return view;
    }

    static LockResult skip(Cursor c, long amount, byte[] limitKey, boolean inclusive)
        throws IOException
    {
        if (amount == 0 || limitKey == null) {
            return c.skip(amount);
        }

        if (amount > 0) {
            if (inclusive) while (true) {
                LockResult result = c.nextLe(limitKey);
                if (c.key() == null || --amount <= 0) {
                    return result;
                }
                if (result == LockResult.ACQUIRED) {
                    c.link().unlock();
                }
            } else while (true) {
                LockResult result = c.nextLt(limitKey);
                if (c.key() == null || --amount <= 0) {
                    return result;
                }
                if (result == LockResult.ACQUIRED) {
                    c.link().unlock();
                }
            }
        } else {
            if (inclusive) while (true) {
                LockResult result = c.previousGe(limitKey);
                if (c.key() == null || ++amount >= 0) {
                    return result;
                }
                if (result == LockResult.ACQUIRED) {
                    c.link().unlock();
                }
            } else while (true) {
                LockResult result = c.previousGt(limitKey);
                if (c.key() == null || ++amount >= 0) {
                    return result;
                }
                if (result == LockResult.ACQUIRED) {
                    c.link().unlock();
                }
            }
        }
    }

    /**
     * Simple skip implementation which doesn't support skip by zero.
     *
     * @throws IllegalArgumentException when skip amount is zero
     */
    static LockResult doSkip(Cursor c, long amount) throws IOException {
        if (amount == 0) {
            throw new IllegalArgumentException("Skip is zero");
        }

        if (amount > 0) while (true) {
            LockResult result = c.next();
            if (c.key() == null || --amount <= 0) {
                return result;
            }
            if (result == LockResult.ACQUIRED) {
                c.link().unlock();
            }
        } else while (true) {
            LockResult result = c.previous();
            if (c.key() == null || ++amount >= 0) {
                return result;
            }
            if (result == LockResult.ACQUIRED) {
                c.link().unlock();
            }
        }
    }

    static final String toString(Index ix) {
        StringBuilder b = new StringBuilder(ix.getClass().getName());
        b.append('@').append(Integer.toHexString(ix.hashCode()));
        b.append(" {");
        String nameStr = ix.getNameString();
        if (nameStr != null) {
            b.append("name").append(": ").append(nameStr);
            b.append(", ");
        }
        b.append("id").append(": ").append(ix.getId());
        return b.append('}').toString();
    }
}
