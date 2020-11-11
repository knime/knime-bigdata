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
 *   Created on 03.07.2015 by koetter
 */
package org.knime.bigdata.spark.local.node.create;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.bigdata.spark.local.node.create.utils.CreateConnectionInformationPortUtil;
import org.knime.bigdata.spark.local.node.create.utils.CreateDatabaseConnectionPortUtil;
import org.knime.bigdata.spark.local.node.create.utils.CreateLocalBDEPortUtil;
import org.knime.core.node.port.database.DatabaseConnectionPortObject;

/**
 * Node model for the "Create Local Big Data Environment" node using a Hive {@link DatabaseConnectionPortObject} and a
 * file system {@link ConnectionInformationPortObject} output port.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class LocalEnvironmentCreatorNodeModel extends AbstractLocalEnvironmentCreatorNodeModel {

    private final CreateDatabaseConnectionPortUtil m_dbPortUtil = new CreateDatabaseConnectionPortUtil();

    private final CreateConnectionInformationPortUtil m_fsPortUtil = new CreateConnectionInformationPortUtil();

	/**
	 * Constructor.
	 */
	LocalEnvironmentCreatorNodeModel() {
		super(CreateDatabaseConnectionPortUtil.PORT_TYPE, CreateConnectionInformationPortUtil.PORT_TYPE, false);
	}

    @Override
    protected CreateLocalBDEPortUtil getDatabasePortUtil() {
        return m_dbPortUtil;
    }

    @Override
    protected CreateLocalBDEPortUtil getFileSystemPortUtil() {
        return m_fsPortUtil;
    }

}
