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

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class BasicJoinTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(BasicJoinTest.class.getName());
    }

    private static Database mDb;
    private static Table<Company> mCompany;
    private static Table<Department> mDepartment;
    private static Table<Employee> mEmployee;

    private Table mJoin;

    @BeforeClass
    public static void setupAll() throws Exception {
        mDb = Database.open(new DatabaseConfig());
        mCompany = mDb.openTable(Company.class);
        mDepartment = mDb.openTable(Department.class);
        mEmployee = mDb.openTable(Employee.class);
        Filler.fillCompany(mCompany, mDepartment, mEmployee);
    }

    @AfterClass
    public static void teardownAll() throws Exception {
        if (mDb != null) {
            mDb.close();
            mDb = null;
        }
        mCompany = null;
        mDepartment = null;
        mEmployee = null;
    }

    @After
    public void teardown() {
        mJoin = null;
    }

    @Test
    public void identity() throws Exception {
        try (var scanner = Table.join().derive("{a=1}").newScanner(null)) {
            assertEquals(1, scanner.row().get_int("a"));
        }
    }

    @Test
    public void crossJoin() throws Exception {
        join("department : employee");

        var plan = """
            - nested loops join
              - first
                - full scan over primary key: org.cojen.tupl.table.join.Department
                  key columns: +id
              - join
                - full scan over primary key: org.cojen.tupl.table.join.Employee
                  key columns: +id
            """;

        var results = new String[] {
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=null, country=Germany, lastName=Williams}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=null, country=Germany, lastName=Williams}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=null, country=Germany, lastName=Williams}}",
            "{department={id=35, companyId=2, name=Marketing}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=35, companyId=2, name=Marketing}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=35, companyId=2, name=Marketing}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=35, companyId=2, name=Marketing}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=35, companyId=2, name=Marketing}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department={id=35, companyId=2, name=Marketing}, employee={departmentId=null, country=Germany, lastName=Williams}}",
        };

        eval(plan, results, "{*}");
    }

    @Test
    public void innerJoin() throws Exception {
        join("employee : department");

        var plan = """
            - nested loops join
              - first
                - full scan over primary key: org.cojen.tupl.table.join.Employee
                  key columns: +id
                assignments: ?1 = employee.departmentId
              - join
                - load one using primary key: org.cojen.tupl.table.join.Department
                  key columns: +id
                  filter: id == ?1
            """;

        var results = new String[] {
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=Germany, lastName=Smith}}",
        };

        eval(plan, results, "department.id == employee.departmentId");
        eval(plan, results, "employee.departmentId == department.id");
        eval(plan, results, "employee.departmentId == department.id" +
             " && department.id == employee.departmentId");

        // Should still loop over employee first, because the overall plan is better.
        join("department : employee");

        eval(plan, results, "department.id == employee.departmentId");
        eval(plan, results, "employee.departmentId == department.id");

        join("employee :: department");

        eval(plan, results, "department.id == employee.departmentId");
        eval(plan, results, "employee.departmentId == department.id");

        // Force a different join order.
        join("department :: employee");

        plan = """
            - nested loops join
              - first
                - full scan over primary key: org.cojen.tupl.table.join.Department
                  key columns: +id
                assignments: ?1 = department.id
              - join
                - filter: departmentId == ?1
                  - full scan over primary key: org.cojen.tupl.table.join.Employee
                    key columns: +id
            """;

        eval(plan, results, "department.id == employee.departmentId");
        eval(plan, results, "employee.departmentId == department.id");
    }

    @Test
    public void leftOuterJoin() throws Exception {
        join("employee >: department");

        var plan = """
            - nested loops join
              - first
                - full scan over primary key: org.cojen.tupl.table.join.Employee
                  key columns: +id
                assignments: ?1 = employee.departmentId
              - outer join
                - load one using primary key: org.cojen.tupl.table.join.Department
                  key columns: +id
                  filter: id == ?1
            """;

        var results = new String[] {
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department=null, employee={departmentId=null, country=Germany, lastName=Williams}}",
        };

        eval(plan, results, "department.id == employee.departmentId");
        eval(plan, results, "employee.departmentId == department.id");

        join("department >: employee");

        plan = """
            - nested loops join
              - first
                - full scan over primary key: org.cojen.tupl.table.join.Department
                  key columns: +id
                assignments: ?1 = department.id
              - outer join
                - filter: departmentId == ?1
                  - full scan over primary key: org.cojen.tupl.table.join.Employee
                    key columns: +id
            """;

        results = new String[] {
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department={id=35, companyId=2, name=Marketing}, employee=null}",
        };

        eval(plan, results, "department.id == employee.departmentId");
        eval(plan, results, "employee.departmentId == department.id");
    }

    @Test
    public void rightOuterJoin() throws Exception {
        join("employee :< department");

        var plan = """
            - nested loops join
              - first
                - full scan over primary key: org.cojen.tupl.table.join.Department
                  key columns: +id
                assignments: ?1 = department.id
              - outer join
                - filter: departmentId == ?1
                  - full scan over primary key: org.cojen.tupl.table.join.Employee
                    key columns: +id
            """;

        var results = new String[] {
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department={id=35, companyId=2, name=Marketing}, employee=null}",
        };

        eval(plan, results, "department.id == employee.departmentId");
        eval(plan, results, "employee.departmentId == department.id");

        join("department :< employee");

        plan = """
            - nested loops join
              - first
                - full scan over primary key: org.cojen.tupl.table.join.Employee
                  key columns: +id
                assignments: ?1 = employee.departmentId
              - outer join
                - load one using primary key: org.cojen.tupl.table.join.Department
                  key columns: +id
                  filter: id == ?1
            """;

        results = new String[] {
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department=null, employee={departmentId=null, country=Germany, lastName=Williams}}",
        };

        eval(plan, results, "department.id == employee.departmentId");
        eval(plan, results, "employee.departmentId == department.id");
    }

    @Test
    public void fullOuterJoin() throws Exception {
        join("employee >:< department");

        var plan = """
            - disjoint union
              - nested loops join
                - first
                  - full scan over primary key: org.cojen.tupl.table.join.Employee
                    key columns: +id
                  assignments: ?1 = employee.departmentId
                - outer join
                  - load one using primary key: org.cojen.tupl.table.join.Department
                    key columns: +id
                    filter: id == ?1
              - nested loops join
                - first
                  - full scan over primary key: org.cojen.tupl.table.join.Department
                    key columns: +id
                  assignments: ?1 = department.id
                - anti join
                  - exists
                    - filter: departmentId == ?1
                      - full scan over primary key: org.cojen.tupl.table.join.Employee
                        key columns: +id
            """;

        var results = new String[] {
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department=null, employee={departmentId=null, country=Germany, lastName=Williams}}",
            "{department={id=35, companyId=2, name=Marketing}, employee=null}",
        };

        eval(plan, results, "department.id == employee.departmentId");
        eval(plan, results, "employee.departmentId == department.id");

        join("department >:< employee");

        plan = """
            - disjoint union
              - nested loops join
                - first
                  - full scan over primary key: org.cojen.tupl.table.join.Department
                    key columns: +id
                  assignments: ?1 = department.id
                - outer join
                  - filter: departmentId == ?1
                    - full scan over primary key: org.cojen.tupl.table.join.Employee
                      key columns: +id
              - nested loops join
                - first
                  - full scan over primary key: org.cojen.tupl.table.join.Employee
                    key columns: +id
                  assignments: ?1 = employee.departmentId
                - anti join
                  - exists
                    - load one using primary key: org.cojen.tupl.table.join.Department
                      key columns: +id
                      filter: id == ?1
            """;

        results = new String[] {
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department={id=35, companyId=2, name=Marketing}, employee=null}",
            "{department=null, employee={departmentId=null, country=Germany, lastName=Williams}}",
        };

        eval(plan, results, "department.id == employee.departmentId");
        eval(plan, results, "employee.departmentId == department.id");
    }

    @Test
    public void filterCrossJoin() throws Exception {
        join("employee : department");

        var plan = """
            - nested loops join
              - first
                - load one using primary key: org.cojen.tupl.table.join.Department
                  key columns: +id
                  filter: id == ?1
              - join
                - full scan over primary key: org.cojen.tupl.table.join.Employee
                  key columns: +id
            """;

        var results = new String[] {
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=null, country=Germany, lastName=Williams}}",
        };

        eval(plan, results, "department.id == ?", 31);

        plan = """
            - nested loops join
              - first
                - load one using primary key: org.cojen.tupl.table.join.Employee
                  key columns: +id
                  filter: id == ?1
              - join
                - full scan over primary key: org.cojen.tupl.table.join.Department
                  key columns: +id
            """;

        results = new String[] { };

        eval(plan, results, "employee.id == ?", 999);
    }

    @Test
    public void filterInnerJoin() throws Exception {
        join("employee : department");

        var plan = """
            - nested loops join
              - first
                - load one using primary key: org.cojen.tupl.table.join.Department
                  key columns: +id
                  filter: id == ?1
                assignments: ?2 = department.id
              - join
                - filter: departmentId == ?2
                  - full scan over primary key: org.cojen.tupl.table.join.Employee
                    key columns: +id
            """;

        var results = new String[] {
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=Germany, lastName=Smith}}",
        };
 
        eval(plan, results, "department.id == employee.departmentId && department.id == ?", 34);

        // This next one has a broken join specification.

        // TODO: The range scans should instead be LoadOne.

        plan = """
            - nested loops join
              - first
                - full scan over primary key: org.cojen.tupl.table.join.Employee
                  key columns: +id
                assignments: ?2 = employee.departmentId
              - join
                - range union
                  - range scan over primary key: org.cojen.tupl.table.join.Department
                    key columns: +id
                    range: id >= ?2 .. id <= ?2
                  - range scan over primary key: org.cojen.tupl.table.join.Department
                    key columns: +id
                    range: id >= ?1 .. id <= ?1
            """;

        results = new String[] {
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=null, country=Germany, lastName=Williams}}",
        };

        eval(plan, results, "employee.departmentId == department.id || department.id == ?", 34);
    }

    @Test
    public void filterLeftOuterJoin() throws Exception {
        join("employee >: department");

        var plan = """
            - nested loops join
              - first
                - full scan over primary key: org.cojen.tupl.table.join.Employee
                  key columns: +id
                assignments: ?2 = employee.departmentId
              - outer join
                - filter: id == ?1
                  - load one using primary key: org.cojen.tupl.table.join.Department
                    key columns: +id
                    filter: id == ?2
            """;

        var results = new String[] {
            "{department=null, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department=null, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department=null, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department=null, employee={departmentId=null, country=Germany, lastName=Williams}}",
        };

        eval(plan, results, "department.id == employee.departmentId && department.id == ?", 34);

        // This next one has a broken join specification.

        // TODO: The range scans should instead be LoadOne.

        plan = """
            - nested loops join
              - first
                - full scan over primary key: org.cojen.tupl.table.join.Employee
                  key columns: +id
                assignments: ?2 = employee.departmentId
              - outer join
                - range union
                  - range scan over primary key: org.cojen.tupl.table.join.Department
                    key columns: +id
                    range: id >= ?2 .. id <= ?2
                  - range scan over primary key: org.cojen.tupl.table.join.Department
                    key columns: +id
                    range: id >= ?1 .. id <= ?1
            """;

        results = new String[] {
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=null, country=Germany, lastName=Williams}}",
        };

        eval(plan, results, "employee.departmentId == department.id || department.id == ?", 34);
    }

    @Test
    public void filterFullOuterJoin() throws Exception {
        join("employee >:< department");

        var plan = """
            - disjoint union
              - nested loops join
                - first
                  - full scan over primary key: org.cojen.tupl.table.join.Employee
                    key columns: +id
                  assignments: ?2 = employee.departmentId
                - outer join
                  - filter: id == ?1
                    - load one using primary key: org.cojen.tupl.table.join.Department
                      key columns: +id
                      filter: id == ?2
              - nested loops join
                - first
                  - load one using primary key: org.cojen.tupl.table.join.Department
                    key columns: +id
                    filter: id == ?1
                  assignments: ?2 = department.id
                - anti join
                  - exists
                    - filter: departmentId == ?2
                      - full scan over primary key: org.cojen.tupl.table.join.Employee
                        key columns: +id
            """;

        var results = new String[] {
            "{department=null, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department=null, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department=null, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department=null, employee={departmentId=null, country=Germany, lastName=Williams}}",
        };

        eval(plan, results, "department.id == employee.departmentId && department.id == ?", 34);

        join("department >:< employee");

        plan = """
            - disjoint union
              - nested loops join
                - first
                  - load one using primary key: org.cojen.tupl.table.join.Department
                    key columns: +id
                    filter: id == ?1
                  assignments: ?2 = department.id
                - outer join
                  - filter: departmentId == ?2
                    - full scan over primary key: org.cojen.tupl.table.join.Employee
                      key columns: +id
              - nested loops join
                - first
                  - full scan over primary key: org.cojen.tupl.table.join.Employee
                    key columns: +id
                  assignments: ?2 = employee.departmentId
                - anti join
                  - exists
                    - filter: id == ?1
                      - load one using primary key: org.cojen.tupl.table.join.Department
                        key columns: +id
                        filter: id == ?2
            """;

        results = new String[] {
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department=null, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department=null, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department=null, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department=null, employee={departmentId=null, country=Germany, lastName=Williams}}",
        };

        eval(plan, results, "department.id == employee.departmentId && department.id == ?", 34);

        // This next one has a broken join specification.

        // TODO: The range scans should instead be LoadOne.

        plan = """
            - disjoint union
              - filter: department.id == employee.departmentId || department.id == ?1
                - nested loops join
                  - first
                    - full scan over primary key: org.cojen.tupl.table.join.Department
                      key columns: +id
                  - outer join
                    - full scan over primary key: org.cojen.tupl.table.join.Employee
                      key columns: +id
              - nested loops join
                - first
                  - full scan over primary key: org.cojen.tupl.table.join.Employee
                    key columns: +id
                  assignments: ?2 = employee.departmentId
                - anti join
                  - exists
                    - range union
                      - range scan over primary key: org.cojen.tupl.table.join.Department
                        key columns: +id
                        range: id >= ?2 .. id <= ?2
                      - range scan over primary key: org.cojen.tupl.table.join.Department
                        key columns: +id
                        range: id >= ?1 .. id <= ?1
            """;

        results = new String[] {
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=null, country=Germany, lastName=Williams}}",
        };

        eval(plan, results, "department.id == employee.departmentId || department.id == ?", 34);
    }

    @Test
    public void misc() throws Exception {
        join("employee : department");

        // TODO: Should apply the filter before loading by primary key. Or should see the
        // equijoin and move the filter to an earlier level.

        String plan = """
            - nested loops join
              - first
                - filter: employee.id != employee.lastName
                  - full scan over primary key: org.cojen.tupl.table.join.Employee
                    key columns: +id
                assignments: ?2 = employee.departmentId
              - join
                - filter: id != ?1
                  - load one using primary key: org.cojen.tupl.table.join.Department
                    key columns: +id
                    filter: id == ?2
            """;

        var results = new String[] {
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=Germany, lastName=Smith}}",
        };

        eval(plan, results,
             "employee.departmentId == department.id " +
             "&& employee.id != employee.lastName " +
             "&& department.id != ?1", 31);

        plan = """
            - nested loops join
              - first
                - filter: employee.id != employee.lastName
                  - full scan over primary key: org.cojen.tupl.table.join.Employee
                    key columns: +id
              - join
                - filter: id != ?1
                  - full scan over primary key: org.cojen.tupl.table.join.Department
                    key columns: +id
            """;

        results = new String[] {
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=35, companyId=2, name=Marketing}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=35, companyId=2, name=Marketing}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=35, companyId=2, name=Marketing}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=35, companyId=2, name=Marketing}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department={id=35, companyId=2, name=Marketing}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=null, country=Germany, lastName=Williams}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=null, country=Germany, lastName=Williams}}",
            "{department={id=35, companyId=2, name=Marketing}, employee={departmentId=null, country=Germany, lastName=Williams}}",
        };

        eval(plan, results, "employee.id != employee.lastName && department.id != ?", 31);

        plan = """
            - filter: (employee.departmentId == department.id && employee.id != employee.lastName) || department.name == ?1
              - nested loops join
                - first
                  - full scan over primary key: org.cojen.tupl.table.join.Employee
                    key columns: +id
                - join
                  - full scan over primary key: org.cojen.tupl.table.join.Department
                    key columns: +id
            """;
        results = new String[] {
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=null, country=Germany, lastName=Williams}}",
        };
            
        eval(plan, results,
             "(employee.departmentId == department.id" +
             " && employee.id != employee.lastName)" +
             " || department.name == ?", "Sales");

        plan = """
            - filter: (employee.departmentId == department.id && employee.id != employee.lastName) || department.name == ?2
              - nested loops join
                - first
                  - filter: id != ?1 || name == ?2
                    - full scan over primary key: org.cojen.tupl.table.join.Department
                      key columns: +id
                - join
                  - full scan over primary key: org.cojen.tupl.table.join.Employee
                    key columns: +id
            """;

        results = new String[] {
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{department={id=31, companyId=1, name=Sales}, employee={departmentId=null, country=Germany, lastName=Williams}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{department={id=33, companyId=2, name=Engineering}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{department={id=34, companyId=1, name=Clerical}, employee={departmentId=34, country=Germany, lastName=Smith}}",
        };

        eval(plan, results,
             "(employee.departmentId == department.id" +
             " && employee.id != employee.lastName" +
             " && department.id != ?) || department.name == ?", 31, "Sales");
    }

    @Test
    public void selfJoin() throws Exception {
        mJoin = mDb.openJoinTable(EmployeeJoinEmployee.class, "first : second");

        String plan = """
            - nested loops join
              - first
                - full scan over primary key: org.cojen.tupl.table.join.Employee
                  key columns: +id
                assignments: ?1 = first.country, ?2 = first.id
              - join
                - filter: country == ?1
                  - range scan over primary key: org.cojen.tupl.table.join.Employee
                    key columns: +id
                    range: id > ?2 ..
            """;

        var results = new String[] {
            "{first={departmentId=31, country=Australia, lastName=Rafferty}, second={departmentId=33, country=Australia, lastName=Jones}}",
            "{first={departmentId=31, country=Australia, lastName=Rafferty}, second={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{first={departmentId=33, country=Australia, lastName=Jones}, second={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{first={departmentId=34, country=Germany, lastName=Smith}, second={departmentId=null, country=Germany, lastName=Williams}}",
        };

        eval(plan, results, "first.country == second.country && first.id < second.id");
    }

    @Test
    public void projection() throws Exception {
        join("employee : department");

        var plan = """
            - nested loops join
              - first
                - full scan over primary key: org.cojen.tupl.table.join.Employee
                  key columns: +id
                assignments: ?1 = employee.departmentId
              - join
                - load one using primary key: org.cojen.tupl.table.join.Department
                  key columns: +id
                  filter: id == ?1
            """;

        var results = new String[] {
            "{department={name=Sales}, employee={departmentId=31, country=Australia}}",
            "{department={name=Engineering}, employee={departmentId=33, country=Australia}}",
            "{department={name=Engineering}, employee={departmentId=33, country=Australia}}",
            "{department={name=Clerical}, employee={departmentId=34, country=United States}}",
            "{department={name=Clerical}, employee={departmentId=34, country=Germany}}",
        };

        eval(plan, results,
             "{department.name, employee.country} department.id == employee.departmentId");

        join3("company >: department : employee");

        plan = """
            - nested loops join
              - first
                - full scan over primary key: org.cojen.tupl.table.join.Company
                  key columns: +id
              - outer join
                - filter: company.id == department.companyId
                  - full scan over primary key: org.cojen.tupl.table.join.Department
                    key columns: +id
                assignments: ?1 = department.id
              - join
                - filter: departmentId == ?1
                  - full scan over primary key: org.cojen.tupl.table.join.Employee
                    key columns: +id
            """;

        results = new String[] {
            "{company={id=1, name=Hooli}, department={id=31, companyId=1, name=Sales}, employee={country=Australia}}",
            "{company={id=1, name=Hooli}, department={id=34, companyId=1, name=Clerical}, employee={country=United States}}",
            "{company={id=1, name=Hooli}, department={id=34, companyId=1, name=Clerical}, employee={country=Germany}}",
            "{company={id=2, name=Initech}, department={id=33, companyId=2, name=Engineering}, employee={country=Australia}}",
            "{company={id=2, name=Initech}, department={id=33, companyId=2, name=Engineering}, employee={country=Australia}}",
        };

        eval(plan, results,
             "{company.name, department.name, employee.country} " +
             "department.id == employee.departmentId && company.id == department.companyId");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void generated() throws Exception {
        // Test against a generated join class.

        mJoin = Table.join("emp : dept", mEmployee, mDepartment);
        assertTrue(Row.class.isAssignableFrom(mJoin.rowType()));

        // First run a basic test like with the innerJoin test.

        var plan = """
            - nested loops join
              - first
                - full scan over primary key: org.cojen.tupl.table.join.Employee
                  key columns: +id
                assignments: ?1 = emp.departmentId
              - join
                - load one using primary key: org.cojen.tupl.table.join.Department
                  key columns: +id
                  filter: id == ?1
            """;

        var results = new String[] {
            "{dept={id=31, companyId=1, name=Sales}, emp={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{dept={id=33, companyId=2, name=Engineering}, emp={departmentId=33, country=Australia, lastName=Jones}}",
            "{dept={id=33, companyId=2, name=Engineering}, emp={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{dept={id=34, companyId=1, name=Clerical}, emp={departmentId=34, country=United States, lastName=Robinson}}",
            "{dept={id=34, companyId=1, name=Clerical}, emp={departmentId=34, country=Germany, lastName=Smith}}",
        };

        eval(true, plan, results, "dept.id == emp.departmentId");

        // Test again with a view.

        mJoin = mJoin.derive(mJoin.rowType(), "dept.id < ?", 34);
        assertTrue(Row.class.isAssignableFrom(mJoin.rowType()));

        plan = """
            - nested loops join
              - first
                - reverse range scan over primary key: org.cojen.tupl.table.join.Department
                  key columns: +id
                  range: .. id < ?1
                assignments: ?2 = dept.id
              - join
                - filter: departmentId == ?2
                  - full scan over primary key: org.cojen.tupl.table.join.Employee
                    key columns: +id
            """;

        results = new String[] {
            "{dept={id=33, companyId=2, name=Engineering}, emp={departmentId=33, country=Australia, lastName=Jones}}",
            "{dept={id=33, companyId=2, name=Engineering}, emp={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{dept={id=31, companyId=1, name=Sales}, emp={departmentId=31, country=Australia, lastName=Rafferty}}",
        };

        eval(true, plan, results, "dept.id == emp.departmentId");

        // Test accessing column paths.

        assertTrue(Row.class.isAssignableFrom(Department.class));
        assertFalse(Row.class.isAssignableFrom(Employee.class));

        String queryStr = "dept.id == ? && emp.lastName == ?";

        try (var scanner = mJoin.newScanner(null, queryStr, 31, "Williams")) {
            for (var row = (Row) scanner.row(); row != null; row = (Row) scanner.step()) {
                assertEquals(int.class, row.columnType("dept.id"));
                assertEquals(Integer.class, row.columnType("dept.companyId"));
                assertEquals(String.class, row.columnType("dept.name"));

                assertEquals("id", row.columnMethodName("dept.id"));
                assertEquals("companyId", row.columnMethodName("dept.companyId"));
                assertEquals("name", row.columnMethodName("dept.name"));

                assertEquals(31, row.get_int("dept.id"));
                assertEquals(1, row.get_int("dept.companyId"));
                assertEquals("Sales", row.getString("dept.name"));

                try {
                    row.get("dept.fake.id");
                    fail();
                } catch (IllegalArgumentException e) {
                    assertEquals("Column name isn't found: fake.id", e.getMessage());
                }

                row.set("dept.id", 123L);
                row.set("dept.companyId", (Integer) null);
                row.set("dept.name", "none");

                assertEquals(123, row.get_int("dept.id"));
                assertEquals(null, row.getInteger("dept.companyId"));
                assertEquals("none", row.getString("dept.name"));

                assertEquals(int.class, row.columnType("emp.id"));
                assertEquals(Integer.class, row.columnType("emp.departmentId"));
                assertEquals(String.class, row.columnType("emp.country"));
                assertEquals(String.class, row.columnType("emp.lastName"));

                assertEquals("id", row.columnMethodName("emp.id"));
                assertEquals("departmentId", row.columnMethodName("emp.departmentId"));
                assertEquals("country", row.columnMethodName("emp.country"));
                assertEquals("lastName", row.columnMethodName("emp.lastName"));

                assertEquals(null, row.get("emp.departmentId"));
                assertEquals("Germany", row.get("emp.country"));
                assertEquals("Williams", row.get("emp.lastName"));

                row.set("emp.id", 10);
                row.set("emp.departmentId", 100);
                row.set("emp.country", "nowhere");
                row.set("emp.lastName", "what");

                assertEquals(10, row.get("emp.id"));
                assertEquals(100, row.get("emp.departmentId"));
                assertEquals("nowhere", row.get("emp.country"));
                assertEquals("what", row.get("emp.lastName"));
            }
        }

        // Test path access against a joined row which is null.

        {
            var row = (Row) mJoin.newRow();

            assertNull(row.getInteger("dept.id"));
            assertNull(row.getInteger("dept.companyId"));

            try {
                row.get_int("dept.companyId");
                fail();
            } catch (ConversionException e) {
                assertEquals("Column path joins to a null row: dept", e.getMessage());
            }

            try {
                row.get_int("dept.id");
                fail();
            } catch (ConversionException e) {
                assertEquals("Column path joins to a null row: dept", e.getMessage());
            }
        }
    }

    private void join(String spec) throws Exception {
        mJoin = mDb.openJoinTable(EmployeeJoinDepartment.class, spec);
    }

    private void join3(String spec) throws Exception {
        mJoin = mDb.openJoinTable(EmployeeJoinDepartmentJoinCompany.class, spec);
    }

    private void eval(String plan, String[] results, String queryStr, Object... args)
        throws Exception 
    {
        eval(false, plan, results, queryStr, args);
    }

    @SuppressWarnings("unchecked")
    private void eval(boolean isRow, String plan, String[] results, String queryStr, Object... args)
        throws Exception 
    {
        Query query = mJoin.query(queryStr);
        assertEquals(mJoin.rowType(), query.rowType());
        assertEquals(args.length, query.argumentCount());

        String actualPlan = query.scannerPlan(null, args).toString();
        assertEquals(plan, actualPlan);

        int resultNum = 0;
        var numRef = new int[1];
        var nameRef = new String[1];

        try (var scanner = mJoin.newScanner(null, queryStr, args)) {
            for (var row = scanner.row(); row != null; row = scanner.step(row)) {
                if (isRow) {
                    assertTrue(row instanceof Row);
                }

                String result = results[resultNum++];
                String actualResult = row.toString();
                assertEquals(result, actualResult);

                numRef[0] = 0;

                mJoin.forEach(row, (r, name, value) -> {
                    assertTrue(result.contains(name + '=' + value));
                    numRef[0]++;

                    assertTrue(mJoin.isSet(r, name));

                    nameRef[0] = name;
                });

                assertTrue(numRef[0] > 0);

                mJoin.unsetRow(row);
                assertFalse(mJoin.isSet(row, nameRef[0]));
                try {
                    mJoin.isSet(row, "xxxxxxxxx");
                    fail();
                } catch (IllegalArgumentException e) {
                    assertTrue(e.getMessage().contains("Unknown column"));
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void dump(String query, Object... args) throws Exception {
        System.out.println(query);
        System.out.println(mJoin.query(query).scannerPlan(null, args));

        try (var scanner = mJoin.newScanner(null, query, args)) {
            for (var row = scanner.row(); row != null; row = scanner.step(row)) {
                System.out.println("\"" + row + "\",");
            }
        }
    }
}
