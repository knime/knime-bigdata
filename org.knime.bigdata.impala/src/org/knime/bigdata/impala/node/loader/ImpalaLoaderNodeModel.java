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
package org.knime.bigdata.impala.node.loader;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.bigdata.hdfs.filehandler.HDFSRemoteFileHandler;
import org.knime.bigdata.hive.utility.AbstractLoaderNodeModel;
import org.knime.bigdata.impala.utility.ImpalaUtility;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionPortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionSettings;

/**
 * Model for the Impala Loader node.
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
class ImpalaLoaderNodeModel extends AbstractLoaderNodeModel {

    ImpalaLoaderNodeModel() {
        super(true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        super.configure(inSpecs);

        //check Impala specific connection settings
        final ConnectionInformationPortObjectSpec object = (ConnectionInformationPortObjectSpec)inSpecs[0];
        final ConnectionInformation connInfo = object.getConnectionInformation();
        final DatabaseConnectionSettings connSettings =
                ((DatabaseConnectionPortObjectSpec)inSpecs[2]).getConnectionSettings(getCredentialsProvider());

        if (!(connSettings.getUtility() instanceof ImpalaUtility)) {
            throw new InvalidSettingsException("Only Impala database connections are supported");
        }
        if (!HDFSRemoteFileHandler.isSupportedConnection(connInfo)) {
            throw new InvalidSettingsException("HDFS connection required");
        }
        // We cannot provide a spec because it's not clear yet what the DB will return when the imported data
        // is read back into KNIME
        return new PortObjectSpec[]{null};
    }




}
