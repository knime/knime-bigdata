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
 *   Created on 17.10.2016 by koetter
 */
package org.knime.bigdata.hive.utility;

import org.knime.core.node.port.database.StatementManipulator;
import org.knime.core.node.port.database.tablecreator.DBTableCreatorIfNotExistsImpl;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@Deprecated
public class HiveTableCreator extends DBTableCreatorIfNotExistsImpl {

    /**
     * @param sm {@link StatementManipulator}
     * @param schema schema of the table to create
     * @param tableName name of the table to create
     * @param isTempTable <code>true</code> if the table is a temporary table, otherwise <code>false</code>
     */
    public HiveTableCreator(final StatementManipulator sm, final String schema, final String tableName,
        final boolean isTempTable) {
        super(sm, schema, tableName, isTempTable);
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
    protected String getKeyConstraintFragment(final boolean isPrimaryKey) throws Exception {
        throw new Exception("Hive does not support key constraints.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getNotNullFragment(final boolean isNotNull) throws Exception {
        if(isNotNull) {
            throw new Exception("Hive does not support NOT NULL option");
        }
        return super.getNotNullFragment(isNotNull);
    }
}
