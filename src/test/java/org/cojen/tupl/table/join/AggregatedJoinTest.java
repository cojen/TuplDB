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
 * @author Brian S. O'Neill
 */
public class AggregatedJoinTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(AggregatedJoinTest.class.getName());
    }

    private static Database mDb;
    private static Table<Department> mDepartment;
    private static Table<Employee> mEmployee;

    @BeforeClass
    public static void setupAll() throws Exception {
        mDb = Database.open(new DatabaseConfig());
        mDepartment = mDb.openTable(Department.class);
        mEmployee = mDb.openTable(Employee.class);
        Filler.fillCompany(null, mDepartment, mEmployee);
    }

    @AfterClass
    public static void teardownAll() throws Exception {
        if (mDb != null) {
            mDb.close();
            mDb = null;
        }
        mDepartment = null;
        mEmployee = null;
    }

    public interface Agg {
        int count();
        void count(int count);
    }

    @PrimaryKey("companyId")
    public interface AggByCompany extends Agg {
        @Nullable
        Integer companyId();
        void companyId(Integer id);
    }

    public interface EmployeeJoinAgg {
        Employee employee();
        void employee(Employee e);

        Agg agg();
        void agg(Agg agg);
    }

    public interface DepartmentJoinAggByCompany {
        Department department();
        void department(Department d);

        AggByCompany agg();
        void agg(AggByCompany agg);
    }

    public static class Aggregator1<T extends Agg> implements Aggregator<Department, T> {
        private int count;

        @Override
        public Department begin(Department source) {
            count = 1;
            return source;
        }

        @Override
        public Department accumulate(Department source) {
            count++;
            return source;
        }

        @Override
        public T finish(T target) {
            target.count(count);
            return target;
        }
    }

    @Test
    public void joinOrder() throws Exception {
        // Test that aggregated tables are ordered first in the join.

        Table<Agg> aggregated = mDepartment.aggregate(Agg.class, Aggregator1::new);

        String spec = "employee : agg";
        Table<EmployeeJoinAgg> joined = Table.join
            (EmployeeJoinAgg.class, spec, mEmployee, aggregated);

        String plan = """
- nested loops join
  - first
    - aggregate: org.cojen.tupl.table.join.AggregatedJoinTest$Agg
      - full scan over primary key: org.cojen.tupl.table.join.Department
        key columns: +id
  - join
    - full scan over primary key: org.cojen.tupl.table.join.Employee
      key columns: +id
            """;

        var results = new String[] {
            "{agg={count=4}, employee={departmentId=31, country=Australia, lastName=Rafferty}}",
            "{agg={count=4}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{agg={count=4}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
            "{agg={count=4}, employee={departmentId=34, country=United States, lastName=Robinson}}",
            "{agg={count=4}, employee={departmentId=34, country=Germany, lastName=Smith}}",
            "{agg={count=4}, employee={departmentId=null, country=Germany, lastName=Williams}}",
        };

        eval(joined, plan, results, "{*}");

        plan = """
- nested loops join
  - first
    - aggregate: org.cojen.tupl.table.join.AggregatedJoinTest$Agg
      - full scan over primary key: org.cojen.tupl.table.join.Department
        key columns: +id
  - join
    - filter: departmentId == ?1
      - full scan over primary key: org.cojen.tupl.table.join.Employee
        key columns: +id
            """;

        results = new String[] {
            "{agg={count=4}, employee={departmentId=33, country=Australia, lastName=Jones}}",
            "{agg={count=4}, employee={departmentId=33, country=Australia, lastName=Heisenberg}}",
        };

        eval(joined, plan, results, "employee.departmentId == ?", 33);
    }

    @Test
    public void joinOrderWithAggregate() throws Exception {
        // Test that aggregated tables are ordered first in the join.

        Table<AggByCompany> aggregated = mDepartment.aggregate
            (AggByCompany.class, Aggregator1::new);

        String spec = "department : agg";

        Table<DepartmentJoinAggByCompany> joined = Table.join
            (DepartmentJoinAggByCompany.class, spec, mDepartment, aggregated);

        String plan = """
- nested loops join
  - first
    - aggregate: org.cojen.tupl.table.join.AggregatedJoinTest$AggByCompany
      group by: companyId
      - sort: +companyId
        - full scan over primary key: org.cojen.tupl.table.join.Department
          key columns: +id
  - join
    - full scan over primary key: org.cojen.tupl.table.join.Department
      key columns: +id
            """;

        var results = new String[] {
            "{agg={companyId=1, count=2}, department={id=31, companyId=1, name=Sales}}",
            "{agg={companyId=1, count=2}, department={id=33, companyId=2, name=Engineering}}",
            "{agg={companyId=1, count=2}, department={id=34, companyId=1, name=Clerical}}",
            "{agg={companyId=1, count=2}, department={id=35, companyId=2, name=Marketing}}",
            "{agg={companyId=2, count=2}, department={id=31, companyId=1, name=Sales}}",
            "{agg={companyId=2, count=2}, department={id=33, companyId=2, name=Engineering}}",
            "{agg={companyId=2, count=2}, department={id=34, companyId=1, name=Clerical}}",
            "{agg={companyId=2, count=2}, department={id=35, companyId=2, name=Marketing}}",
        };

        eval(joined, plan, results, "{*}");

        plan = """
- nested loops join
  - first
    - aggregate: org.cojen.tupl.table.join.AggregatedJoinTest$AggByCompany
      group by: companyId
      - sort: +companyId
        - full scan over primary key: org.cojen.tupl.table.join.Department
          key columns: +id
    assignments: ?1 = agg.companyId
  - join
    - filter: companyId == ?1
      - full scan over primary key: org.cojen.tupl.table.join.Department
        key columns: +id
            """;

        results = new String[] {
            "{agg={companyId=1, count=2}, department={id=31, companyId=1, name=Sales}}",
            "{agg={companyId=1, count=2}, department={id=34, companyId=1, name=Clerical}}",
            "{agg={companyId=2, count=2}, department={id=33, companyId=2, name=Engineering}}",
            "{agg={companyId=2, count=2}, department={id=35, companyId=2, name=Marketing}}",
        };

        eval(joined, plan, results, "department.companyId == agg.companyId");

        plan = """
- nested loops join
  - first
    - filter: companyId == ?1
      - full scan over primary key: org.cojen.tupl.table.join.Department
        key columns: +id
    assignments: ?2 = department.companyId
  - join
    - aggregate: org.cojen.tupl.table.join.AggregatedJoinTest$AggByCompany
      group by: companyId
      - filter: companyId == ?2
        - full scan over primary key: org.cojen.tupl.table.join.Department
          key columns: +id
            """;

        results = new String[] {
            "{agg={companyId=2, count=2}, department={id=33, companyId=2, name=Engineering}}",
            "{agg={companyId=2, count=2}, department={id=35, companyId=2, name=Marketing}}",
        };

        eval(joined, plan, results,
             "department.companyId == agg.companyId && department.companyId == ?", 2);

        plan = """
- nested loops join
  - first
    - aggregate: org.cojen.tupl.table.join.AggregatedJoinTest$AggByCompany
      group by: companyId
      - filter: companyId == ?1
        - full scan over primary key: org.cojen.tupl.table.join.Department
          key columns: +id
    assignments: ?2 = agg.companyId
  - join
    - filter: companyId == ?2
      - full scan over primary key: org.cojen.tupl.table.join.Department
        key columns: +id
            """;

        results = new String[] {
            "{agg={companyId=2, count=2}, department={id=33, companyId=2, name=Engineering}}",
            "{agg={companyId=2, count=2}, department={id=35, companyId=2, name=Marketing}}",
        };

        eval(joined, plan, results,
             "department.companyId == agg.companyId && agg.companyId == ?", 2);
    }

    @SuppressWarnings("unchecked")
    private void eval(Table join, String plan, String[] results, String queryStr, Object... args)
        throws Exception 
    {
        Query query = join.query(queryStr);
        assertEquals(join.rowType(), query.rowType());
        assertEquals(args.length, query.argumentCount());
        String actualPlan = query.scannerPlan(null, args).toString();
        assertEquals(plan, actualPlan);

        int resultNum = 0;

        try (var scanner = join.newScanner(null, queryStr, args)) {
            for (var row = scanner.row(); row != null; row = scanner.step(row)) {
                String result = results[resultNum++];
                String actualResult = row.toString();
                assertEquals(result, actualResult);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void dump(Table table, String query, Object... args) throws Exception {
        System.out.println(query);
        System.out.println(table.query(query).scannerPlan(null, args));

        try (var scanner = table.newScanner(null, query, args)) {
            for (var row = scanner.row(); row != null; row = scanner.step(row)) {
                System.out.println("\"" + row + "\",");
            }
        }
    }
}
