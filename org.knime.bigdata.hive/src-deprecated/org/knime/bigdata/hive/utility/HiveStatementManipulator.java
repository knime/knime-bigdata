/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
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
 *   Created on 08.08.2014 by koetter
 */
package org.knime.bigdata.hive.utility;

import org.knime.core.node.port.database.StatementManipulator;

/**
 * Statement manipulator for Hive.
 *
 * @author Thorsten Meinl, KNIME AG, Zurich, Switzerland
 */
@Deprecated
public class HiveStatementManipulator extends StatementManipulator {

    /**
     * Constructor of class {@link HiveStatementManipulator}.
     */
   public HiveStatementManipulator() {
       super(true);
   }

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
    public String quoteIdentifier(final String identifier) {
        return getValidColumnName(identifier);
    }

    /**
     * {@inheritDoc}
     * @deprecated
     */
    @Deprecated
    @Override
    public String quoteColumn(final String colName) {
        return getValidColumnName(colName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getValidColumnName(final String colName) {
        // Hive does not support all characters
        return colName.replaceAll("[^0-9a-zA-Z_]", "_");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String forMetadataOnly(final String sql) {
        return limitRows(sql, 0);
    }

    @Override
    public String randomRows(final String sql, final long count) {
        final String tmp = "SELECT * FROM (" + sql + ") " + getTempTableName() + " DISTRIBUTE BY rand() SORT BY rand() LIMIT " + count;
        return tmp;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] createTableAsSelect(final String tableName, final String query) {
        return new String[] {"CREATE TABLE " + quoteIdentifier(tableName) + " AS " + query};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String dropTable(final String tableName, final boolean cascade) {
        //Hive does not support the cascade option
        return super.dropTable(tableName, false);
    }
}