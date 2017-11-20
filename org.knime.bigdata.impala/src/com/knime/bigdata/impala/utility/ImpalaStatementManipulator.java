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
 *   Created on 01.08.2014 by koetter
 */
package com.knime.bigdata.impala.utility;

import org.knime.core.node.port.database.StatementManipulator;

/**
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
public class ImpalaStatementManipulator extends StatementManipulator {

    /**
     * Constructor.
     */
    public ImpalaStatementManipulator() {
        super(true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String unquoteColumn(final String colName) {
        final String unquotedString;
        if (isQuoted(colName)) {
            //if the column name is quoted take it as it is
            unquotedString = colName.substring(1, colName.length());
        } else {
            unquotedString = colName;
        }
        return unquotedString;
    }

    /**
     * @param colName
     * @return
     */
    private boolean isQuoted(final String colName) {
        return colName.startsWith("`") && colName.endsWith("`");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String quoteIdentifier(final String identifier) {
        final String validName = getValidColumnName(identifier);
        return "`" + validName + "`";
    }

    /**
     * {@inheritDoc}
     * @deprecated
     */
    @Deprecated
    @Override
    public String quoteColumn(final String colName) {
        return quoteIdentifier(colName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getValidColumnName(final String colName) {
     // Impala does not support all other characters
        final String cleanedString = colName.replaceAll("[^0-9a-zA-Z_]", "_");
        return cleanedString.toLowerCase();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String forMetadataOnly(final String sql) {
        return limitRows(sql, 0);
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
        //Impala does not support the cascade option
        return super.dropTable(tableName, false);
    }
}