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
package com.knime.bigdata.phoenix.utility;

import org.knime.core.node.port.database.StatementManipulator;

/**
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
public class PhoenixStatementManipulator extends StatementManipulator {
    /**
     * {@inheritDoc}
     */
    @Override
    public String unquoteColumn(final String colName) {
        if (colName == null || colName.isEmpty()) {
            return colName;
        }
        if (colName.startsWith("\"") && colName.endsWith("\"")) {
            final String removedQuotes = colName.substring(1, colName.length() - 1);
            //unqoute quotes
            final String unquotedQuotes = removedQuotes.replaceAll("\"\"", "\"");
            return unquotedQuotes;
        }
        return colName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] createTableAsSelect(final String tableName, final String query) {
        return new String[] {"UPSERT INTO TABLE " + tableName + " " + query};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String quoteIdentifier(final String identifier) {
        return quoteColumn(identifier);
    }

    /**
     * {@inheritDoc}
     * @deprecated
     */
    @Deprecated
    @Override
    public String quoteColumn(final String colName) {
        if (colName == null || colName.isEmpty()) {
            return colName;
        }
        final String newColName;
        if (colName.startsWith("\"") && colName.endsWith("\"")) {
            newColName = colName;
        } else {
            final String quotedQuotes = colName.replaceAll("\"", "\"\"");
            newColName = "\"" + quotedQuotes + "\"";
        }
        return newColName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String forMetadataOnly(final String sql) {
        return limitRows(sql, 0);
    }
}