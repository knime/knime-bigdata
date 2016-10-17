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
 *   Created on 17.10.2016 by koetter
 */
package com.knime.bigdata.phoenix.utility;

import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.tablecreator.DBTableCreatorIfNotExistsImpl;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class PhoenixTableCreator extends DBTableCreatorIfNotExistsImpl {

    /**
     * @param conn a database connection settings object
     * @param schema schema of the table to create
     * @param tableName name of the table to create
     * @param isTempTable <code>true</code> if the table is a temporary table, otherwise <code>false</code>
     */
    protected PhoenixTableCreator(final DatabaseConnectionSettings conn, final String schema, final String tableName,
        final boolean isTempTable) {
        super(conn, schema, tableName, isTempTable);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getTerminalCharacter() {
        return "";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getTempFragment(final boolean isTempTable) {
        if(isTempTable) {
            throw new RuntimeException("Phoenix does not support temporary tables.");
        }
        return super.getTempFragment(isTempTable);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getPrimaryKeyFragment(final boolean isPrimaryKey) {
        if(!isPrimaryKey) {
            throw new RuntimeException("Phoenix only supports primary keys.");
        }
        return super.getPrimaryKeyFragment(isPrimaryKey);
    }
}
