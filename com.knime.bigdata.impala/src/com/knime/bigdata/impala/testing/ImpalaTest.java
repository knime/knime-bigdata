/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   06.05.2014 (thor): created
 */
package com.knime.bigdata.impala.testing;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 *
 * @author Thorsten Meinl, KNIME.com, Zurich, Switzerland
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 */
public class ImpalaTest {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    /**
     * @param args
     */
    public static void main(final String[] args) {
        Connection con = null;
        try {
            Class.forName(driverName);

            //replace "hive" here with the name of the user the queries should run as
//            String jdbcUrl = "jdbc:hive2://192.168.56.101:21050/;auth=noSasl";
//            String userName = "cloudera";
//            String pwd = "";
            String jdbcUrl = "jdbc:hive2://ec2-54-165-140-183.compute-1.amazonaws.com:21050";
            String userName = "knime@cloudera.com";
            String pwd = "Cloudera!!";
            con = DriverManager.getConnection(jdbcUrl, userName, pwd);
            System.out.println("DIRECT CONNECTION");
            testConnection(con);
            con.close();
            System.out.println("KNIME CONNECTION");
            con = createKNIMEConnection(jdbcUrl, userName, pwd);
            testConnection(con);
        } catch (Exception e) {
            System.err.println("GENERAL EXCEPTION: " +e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    System.err.println("Exceptionw hen closing connection: " +e.getMessage());
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }
    }

    private static Connection createKNIMEConnection(final String jdbcUrl, final String userName, final String pwd) throws SQLException {
        Connection con;
        Driver driver = DriverManager.getDriver(jdbcUrl);
        Properties props = new Properties();
        if (userName != null) {
            props.put("user", userName);
        }
        if (pwd != null) {
            props.put("password", pwd);
        }
        con = driver.connect(jdbcUrl, props);
        return con;
    }

    private static void testConnection(final Connection con) throws SQLException {
        Statement stmt = null;
        try {
            stmt = con.createStatement();
            String tableName = "knime_test_impala_driver_table";
            stmt.execute("drop table if exists " + tableName);
            stmt.execute("create table " + tableName + " (key int, value string)");
            // show tables
            String sql = "show tables '" + tableName + "'";
            System.out.println("Running: " + sql);
            ResultSet res = stmt.executeQuery(sql);
            if (res.next()) {
                System.out.println(res.getString(1));
            }
            res.close();
            // describe table
            sql = "describe " + tableName;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1) + "\t" + res.getString(2));
            }
            res.close();
            // load data into table
            // NOTE: filepath has to be local to the hive server
            // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
       //        String filepath = "/tmp/a.txt";
       //        sql = "load data local inpath '" + filepath + "' into table " + tableName;
       //        System.out.println("Running: " + sql);
       //        stmt.execute(sql);

            // select * query
            sql = "select * from " + tableName;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
            }
            res.close();
            // regular hive query
            sql = "select count(1) from " + tableName;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1));
            }
            res.close();
            //drop the table
            stmt.execute("drop table if exists " + tableName);
            System.out.println("Dropping table succeeded");
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }
}
