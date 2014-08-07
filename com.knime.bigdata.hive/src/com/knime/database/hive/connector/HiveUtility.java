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
 *   Created on 08.05.2014 by thor
 */
package com.knime.database.hive.connector;

import org.knime.core.node.port.database.DatabaseUtility;
import org.knime.core.node.port.database.StatementManipulator;

/**
 * Database utility for Hive.
 *
 * @author Thorsten Meinl, KNIME.com, Zurich, Switzerland
 */
public class HiveUtility extends DatabaseUtility {
    private static class HiveStatementManipulator extends StatementManipulator {
        /**
         * {@inheritDoc}
         */
        @Override
        public String unquoteColumn(final String colName) {
            // Hive's JDBC drivers always adds the table name to the column names
            return colName.replaceFirst("^[^\\.]*\\.", "");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String quoteColumn(final String colName) {
            // Hive does not all other characters
            return colName.replaceAll("[^0-9a-zA-Z_]", "_");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String forMetadataOnly(final String sql) {
            return limitRows(sql, 0);
        }
    }

    /**The unique database identifier.*/
    static final String DATABASE_IDENTIFIER = "hive2";

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDatabaseIdentifier() {
        return DATABASE_IDENTIFIER;
    }

    private static final StatementManipulator MANIPULATOR = new HiveStatementManipulator();

    /**
     * {@inheritDoc}
     */
    @Override
    public StatementManipulator getStatementManipulator() {
        return MANIPULATOR;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsDelete() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsUpdate() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsInsert() {
        return false;
    }
}
