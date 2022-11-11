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
 *   Created on 09.05.2014 by thor
 */
package org.knime.bigdata.hive.node.loader;

import org.knime.bigdata.hive.utility.AbstractLoaderNodeModel;
import org.knime.bigdata.hive.utility.HiveUtility;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionPortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionSettings;

/**
 * Model for the Hive Loader node.
 *
 * @author Thorsten Meinl, KNIME AG, Zurich, Switzerland
 */
@Deprecated
class HiveLoaderNodeModel extends AbstractLoaderNodeModel {

    HiveLoaderNodeModel() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs)
            throws InvalidSettingsException {

        super.configure(inSpecs);

        // check Hive specific database connection
        final DatabaseConnectionSettings connSettings =
                ((DatabaseConnectionPortObjectSpec) inSpecs[2])
                .getConnectionSettings(getCredentialsProvider());
        if (!connSettings.getDatabaseIdentifier().contains(HiveUtility.DATABASE_IDENTIFIER)) {
            throw new InvalidSettingsException("Only Hive database connections are supported");
        }
        // We cannot provide a spec because it's not clear yet what the DB will return when the imported data
        // is read back into KNIME
        return new PortObjectSpec[] {null};
    }
}
