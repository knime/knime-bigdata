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

import org.knime.testing.core.AbstractDatabaseJanitor;

/**
 * Creates temporary databases for testing and sets workflow variables accordingly.
 *
 * @author Thorsten Meinl, KNIME.com, Zurich, Switzerland
 */
public class HiveDatabaseJanitor extends AbstractDatabaseJanitor {
    private static final String DB_HOST = "hive.testing.knime.org";

    private static final int DB_PORT = 10000;

    /**
     * Creates a new janitor for PostgreSQL.
     */
    public HiveDatabaseJanitor() {
        super("org.apache.hive.jdbc.HiveDriver", "", DB_HOST, DB_PORT, "hive", "");
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return "Hive test databases";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getID() {
        return "com.knime.database.hive.testing.HiveDatabaseJanitor";
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected String getJDBCUrl(final String dbName) {
        return "jdbc:hive2://" + DB_HOST + ":" + DB_PORT + "/" + dbName;
    }
}
