/*
 *  Copyright (C) 2023 Cojen.org
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

package org.cojen.tupl.table.join;

import org.cojen.tupl.Automatic;
import org.cojen.tupl.Nullable;
import org.cojen.tupl.PrimaryKey;
import org.cojen.tupl.Row;

@org.junit.Ignore
@PrimaryKey("id")
public interface Department extends Row {
    @Automatic
    int id();
    void id(int id);

    String name();
    void name(String s);

    @Nullable
    Integer companyId();
    void companyId(Integer id);
}
