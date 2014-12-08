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
 *   Created on 08.08.2014 by koetter
 */
package com.knime.bigdata.hdfs.node.connector;

import org.knime.base.filehandling.remote.connectioninformation.node.ConnectionInformationNodeModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.bigdata.hdfs.filehandler.HDFSRemoteFileHandler;

/**
 *
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 */
public class HDFSConnectionInformationNodeModel extends ConnectionInformationNodeModel {

    /**
     * @param protocol the protocol
     */
    HDFSConnectionInformationNodeModel() {
        super(HDFSRemoteFileHandler.PROTOCOL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        HDFSRemoteFileHandler.LICENSE_CHECKER.checkLicenseInNode();
        return super.configure(inSpecs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        super.saveSettingsTo(settings);
        //set a default password otherwise a missing password warning appears
        settings.addString("password", "default");
    }
}
