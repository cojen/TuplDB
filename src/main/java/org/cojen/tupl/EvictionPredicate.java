/*
 *  Copyright 2011-2015 Cojen.org
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
 * Class to customize the {@link Index#evict} method behavior.  
 * @author kkreddy
 */
@FunctionalInterface
public interface EvictionPredicate {
    
    /**
     * Default eviction predicate: always deletes entries.
     */
    EvictionPredicate ALWAYS_EVICT = (txn, key, value) -> { return true;};
    
    /**
     * Called after the cursor is positioned at a candidate entry.  
     * Examine the key and value and return true if you want to evict.
     */
    boolean shouldEvict(Transaction txn, byte[] key, byte[] value) throws IOException;
    
}
