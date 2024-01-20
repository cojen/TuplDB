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

import java.io.IOException;

import java.sql.Connection;
import java.sql.SQLException;

import org.cojen.tupl.Table;

/**
 * Creates test data.
 */
@org.junit.Ignore
public class Filler {
    public static void fillCompany(Table<Company> company,
                                   Table<Department> department,
                                   Table<Employee> employee)
        throws IOException
    {
        if (company != null) {
            Object[] companyData = {
                1, "Hooli",
                2, "Initech",
                3, "Home",
            };

            for (int i=0; i<companyData.length; ) {
                var row = company.newRow();
                row.id((Integer) companyData[i++]);
                row.name((String) companyData[i++]);
                company.insert(null, row);
            }
        }

        if (department != null) {
            Object[] deptData = {
                31, "Sales", 1,
                33, "Engineering", 2,
                34, "Clerical", 1,
                35, "Marketing", 2,
            };

            for (int i=0; i<deptData.length; ) {
                var row = department.newRow();
                row.id((Integer) deptData[i++]);
                row.name((String) deptData[i++]);
                row.companyId((Integer) deptData[i++]);
                department.insert(null, row);
            }
        }

        if (employee != null) {
            Object[] empData = {
                "Rafferty", 31, "Australia",
                "Jones", 33, "Australia",
                "Heisenberg", 33, "Australia",
                "Robinson", 34, "United States",
                "Smith", 34, "Germany",
                "Williams", null, "Germany",
            };

            for (int i=0; i<empData.length; ) {
                var row = employee.newRow();
                row.lastName((String) empData[i++]);
                row.departmentId((Integer) empData[i++]);
                row.country((String) empData[i++]);
                employee.insert(null, row);
            }
        }
    }

    public static void fillCompanySQL(Connection con) throws SQLException {
        try (var st = con.createStatement()) {
            st.execute("CREATE TABLE company(CompanyID INT PRIMARY KEY NOT NULL, Name VARCHAR(20) NOT NULL);");
        }

        try (var st = con.createStatement()) {
            st.execute("CREATE TABLE department(DepartmentID INT PRIMARY KEY NOT NULL, Name VARCHAR(20) NOT NULL, CompanyID INT);");
        }

        try (var st = con.createStatement()) {
            st.execute("CREATE TABLE employee(EmployeeID INT PRIMARY KEY NOT NULL, LastName VARCHAR(20) NOT NULL, Country VARCHAR(20) NOT NULL, DepartmentID INT);");
        }

        try (var st = con.createStatement()) {
            st.execute("INSERT INTO company VALUES (1, 'Hooli'), (2, 'Initech'), (3, 'Home');");
        }
        
        try (var st = con.createStatement()) {
            st.execute("INSERT INTO department VALUES (31, 'Sales', 1), (33, 'Engineering', 2), (34, 'Clerical', 1), (35, 'Marketing', 2);");
        }

        try (var st = con.createStatement()) {
            st.execute("INSERT INTO employee VALUES (1001, 'Rafferty', 'Australia', 31), (1002, 'Jones', 'Australia', 33), (1003, 'Heisenberg', 'Australia', 33), (1004, 'Robinson', 'United States', 34), (1005, 'Smith', 'Germany', 34), (1006, 'Williams', 'Germany', NULL);");
        }
    }

    public static void dropSQL(Connection con) throws SQLException {
        try (var st = con.createStatement()) {
            st.execute("DROP SCHEMA PUBLIC CASCADE");
        }
    }
}
