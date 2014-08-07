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
 *   Created on 01.08.2014 by koetter
 */
package com.knime.database.impala.utility;

import org.knime.core.node.port.database.StatementManipulator;

/**
 *
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 */
public class ImpalaStatementManipulator extends StatementManipulator {
    /**
     * {@inheritDoc}
     */
    @Override
    public String unquoteColumn(final String colName) {
        // Impala's JDBC drivers always adds the table name to the column names
        final String unquotedString = colName.replace("`", "");
        return unquotedString.replaceFirst("^[^\\.]*\\.", "");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String quoteColumn(final String colName) {
        // Impala does not all other characters
        final String cleanedString = colName.replaceAll("[^0-9a-zA-Z_]", "_");
        return "`" + cleanedString + "`".toLowerCase();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String forMetadataOnly(final String sql) {
        return limitRows(sql, 0);
    }
}