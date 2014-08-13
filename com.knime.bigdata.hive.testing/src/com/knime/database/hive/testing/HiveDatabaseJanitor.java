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
 *   30.07.2014 (thor): created
 */
package com.knime.database.hive.testing;

import java.lang.reflect.Field;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabaseDriverLoader;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.testing.core.TestrunConfiguration;
import org.knime.testing.core.TestrunJanitor;

/**
 * Creates temporary databases for testing and sets workflow variables accordingly.
 *
 * @author Thorsten Meinl, KNIME.com, Zurich, Switzerland
 */
public class HiveDatabaseJanitor implements TestrunJanitor {
    private static final String JDBC_URL = "jdbc:hive2://testing.knime.org:10000/";

    private String m_dbName;

    /**
     * {@inheritDoc}
     */
    @Override
    public void before(final TestrunConfiguration config) throws Exception {
        DatabaseDriverLoader.registerDriver("org.apache.hive.jdbc.HiveDriver");
        SecureRandom rand = new SecureRandom();

        try (Connection conn = DriverManager.getConnection(JDBC_URL, "hduser", "")) {
            String dbName = "knime_testing_" + Long.toHexString(rand.nextLong()) + Long.toHexString(rand.nextLong());
            Statement stmt = conn.createStatement();
            String sql = "CREATE DATABASE " + dbName;
            stmt.execute(sql);
            m_dbName = dbName;
            NodeLogger.getLogger(getClass()).info("Created temporary Hive testing database " + m_dbName);
            config.addFlowVariable(new FlowVariable("hive-db-name", m_dbName));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void after(final TestrunConfiguration config) throws Exception {
        if (m_dbName != null) {
            Class<DatabaseConnectionSettings> clazz = DatabaseConnectionSettings.class;
            Field mapField = clazz.getDeclaredField("CONNECTION_MAP");
            mapField.setAccessible(true);
            Map<?, Connection> connectionMap = (Map<?, Connection>)mapField.get(null);
            for (Connection conn : connectionMap.values()) {
                if (!conn.isClosed() && conn.getMetaData().getURL().startsWith("jdbc:hive2:")) {
                    conn.close();
                }
            }

            try (Connection conn = DriverManager.getConnection(JDBC_URL, "hduser", "")) {
                Statement stmt = conn.createStatement();
                String sql = "DROP DATABASE " + m_dbName;
                stmt.execute(sql);
                NodeLogger.getLogger(getClass()).info("Deleted temporary Hive testing database " + m_dbName);
            }
        }
    }
}
