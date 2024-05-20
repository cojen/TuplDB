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

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class MappedJoinTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(MappedJoinTest.class.getName());
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

    @Test
    public void basic() throws Exception {
        Table<Emp> emp = mEmployee.map(Emp.class, new ToEmp());
        Table<Dept> dept = mDepartment.map(Dept.class, new ToDept());
        Table<EmpJoinDept> join = Table.join(EmpJoinDept.class, "emp : dept", emp, dept);

        var plan = """
- nested loops join
  - first
    - map: org.cojen.tupl.table.join.MappedJoinTest$Emp
      operation: ToEmp
      - full scan over primary key: org.cojen.tupl.table.join.Employee
        key columns: +id
    assignments: ?1 = emp.deptId
  - join
    - filter: id == ?1
      - map: org.cojen.tupl.table.join.MappedJoinTest$Dept
        operation: ToDept
        - full scan over primary key: org.cojen.tupl.table.join.Department
          key columns: +id
            """;

        var results = new String[] {
            "{dept={id=31, name=Sales}, emp={deptId=31, country=Australia, name=Rafferty}}",
            "{dept={id=33, name=Engineering}, emp={deptId=33, country=Australia, name=Jones}}",
            "{dept={id=33, name=Engineering}, emp={deptId=33, country=Australia, name=Heisenberg}}",
            "{dept={id=34, name=Clerical}, emp={deptId=34, country=United States, name=Robinson}}",
            "{dept={id=34, name=Clerical}, emp={deptId=34, country=Germany, name=Smith}}",
        };

        eval(join, plan, results, "emp.deptId == dept.id");

        plan = """
- nested loops join
  - first
    - map: org.cojen.tupl.table.join.MappedJoinTest$Emp
      operation: ToEmp
      - filter: lastName == ?2
        - full scan over primary key: org.cojen.tupl.table.join.Employee
          key columns: +id
    assignments: ?2 = emp.deptId
  - join
    - filter: id == ?2
      - map: org.cojen.tupl.table.join.MappedJoinTest$Dept
        operation: ToDept
        - full scan over primary key: org.cojen.tupl.table.join.Department
          key columns: +id
            """;

        results = new String[] {
            "{dept={id=34, name=Clerical}, emp={deptId=34, country=Germany, name=Smith}}",
        };

        eval(join, plan, results, "emp.deptId == dept.id && emp.name == ?", "Smith");

        Table<EmpAndDept> flat = join.map(EmpAndDept.class, new ToEmpAndDept()); 

        plan = """
- filter: deptName == ?1
  - map: org.cojen.tupl.table.join.MappedJoinTest$EmpAndDept
    operation: ToEmpAndDept
    - nested loops join
      - first
        - map: org.cojen.tupl.table.join.MappedJoinTest$Emp
          operation: ToEmp
          - full scan over primary key: org.cojen.tupl.table.join.Employee
            key columns: +id
      - join
        - map: org.cojen.tupl.table.join.MappedJoinTest$Dept
          operation: ToDept
          - full scan over primary key: org.cojen.tupl.table.join.Department
            key columns: +id
            """;

        results = new String[] {
            "{deptId=33, deptName=Engineering, empCountry=Australia, empName=Jones}",
            "{deptId=33, deptName=Engineering, empCountry=Australia, empName=Heisenberg}",
        };

        eval(flat, plan, results, "{*, ~empId} deptName == ?", "Engineering");
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
    private static void dump(Table join, String query, Object... args) throws Exception {
        System.out.println(query);
        System.out.println(join.query(query).scannerPlan(null, args));

        try (var scanner = join.newScanner(null, query, args)) {
            for (var row = scanner.row(); row != null; row = scanner.step(row)) {
                System.out.println("\"" + row + "\",");
            }
        }
    }

    public static interface Emp {
        @Hidden
        int id();
        void id(int id);

        String name();
        void name(String s);

        String country();
        void country(String s);

        @Nullable
        Integer deptId();
        void deptId(Integer id);
    }

    public static interface Dept {
        int id();
        void id(int id);

        String name();
        void name(String s);
    }

    public static interface EmpJoinDept {
        Emp emp();
        void emp(Emp e);

        Dept dept();
        void dept(Dept d);
    }

    public static interface EmpAndDept {
        int empId();
        void empId(int id);

        String empName();
        void empName(String s);

        String empCountry();
        void empCountry(String s);

        @Nullable
        Integer deptId();
        void deptId(Integer id);

        String deptName();
        void deptName(String s);
    }

    public static class ToEmp implements Mapper<Employee, Emp> {
        @Override
        public Emp map(Employee source, Emp target) {
            target.id(source.id());
            target.name(source.lastName());
            target.country(source.country());
            target.deptId(source.departmentId());
            return target;
        }

        public static String name_to_lastName(String name) {
            return name;
        }

        @Override
        public QueryPlan.Mapper plan(QueryPlan.Mapper plan) {
            return plan.withOperation(getClass().getSimpleName());
        }
    }

    public static class ToDept implements Mapper<Department, Dept> {
        @Override
        public Dept map(Department source, Dept target) {
            target.id(source.id());
            target.name(source.name());
            return target;
        }

        @Override
        public QueryPlan.Mapper plan(QueryPlan.Mapper plan) {
            return plan.withOperation(getClass().getSimpleName());
        }
    }

    public static class ToEmpAndDept implements Mapper<EmpJoinDept, EmpAndDept> {
        @Override
        public EmpAndDept map(EmpJoinDept source, EmpAndDept target) {
            Integer empDeptId = source.emp().deptId();

            if (empDeptId == null || empDeptId != source.dept().id()) {
                return null;
            }

            target.empId(source.emp().id());
            target.empName(source.emp().name());
            target.empCountry(source.emp().country());
            target.deptId(source.dept().id());
            target.deptName(source.dept().name());
            return target;
        }

        @Override
        public QueryPlan.Mapper plan(QueryPlan.Mapper plan) {
            return plan.withOperation(getClass().getSimpleName());
        }
    }
}
