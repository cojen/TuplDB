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
