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
package org.knime.bigdata.hdfs.node.connector;

import org.knime.base.filehandling.remote.connectioninformation.node.ConnectionInformationNodeModel;
import org.knime.base.filehandling.remote.files.Protocol;
import org.knime.core.node.NodeSettingsWO;

/**
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
public class HDFSConnectionInformationNodeModel extends ConnectionInformationNodeModel {

    /**
     * @param hdfsProtocol
     * @param protocol the protocol
     */
    HDFSConnectionInformationNodeModel(final Protocol hdfsProtocol) {
        super(hdfsProtocol);
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
