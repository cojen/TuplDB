/*
 *  Copyright 2013-2015 Cojen.org
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

/**
 * Defines transaction lock upgrade behavior when using the {@link LockMode#REPEATABLE_READ
 * REPEATABLE_READ} lock mode.
 *
 * <p>Rules ordered from strongest to weakest:
 * <ul>
 * <li>{@link #STRICT} (default)
 * <li>{@link #LENIENT}
 * <li>{@link #UNCHECKED}
 * </ul>
 *
 * @author Brian S O'Neill
 * @see DatabaseConfig#lockUpgradeRule
 */
public enum LockUpgradeRule {
    /**
     * Rule which rejects any shared lock upgrade as {@link LockResult#ILLEGAL illegal}. The
     * lock must first be acquired as {@link LockMode#UPGRADABLE_READ upgradable}.
     */
    STRICT,

    /**
     * Rule which allows an upgrade to succeed, but only when the acting transaction is the
     * sole shared lock owner. If other transactions are also holding the lock, the upgrade
     * attempt is {@link LockResult#ILLEGAL illegal}.
     */
    LENIENT,

    /**
     * Rule which always attempts an upgrade, potentially causing a {@link DeadlockException
     * deadlock} if multiple transactions are making the same attempt.
     */
    UNCHECKED
}
