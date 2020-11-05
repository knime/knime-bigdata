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
package org.knime.bigdata.testing.node.create;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.bigdata.testing.node.create.utils.CreateTestConnectioInformationPortUtil;
import org.knime.bigdata.testing.node.create.utils.CreateTestDatabaseConnectionPortUtil;
import org.knime.bigdata.testing.node.create.utils.CreateTestPortUtil;
import org.knime.core.node.port.database.DatabaseConnectionPortObject;

/**
 * Node model for the "Create Big Data Test Environment" node using a Hive {@link DatabaseConnectionPortObject} and a
 * file system {@link ConnectionInformationPortObject} output port.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class CreateTestEnvironmentNodeModel extends AbstractCreateTestEnvironmentNodeModel {

    private final CreateTestDatabaseConnectionPortUtil m_dbPortUtil = new CreateTestDatabaseConnectionPortUtil();

    private final CreateTestConnectioInformationPortUtil m_fsPortUtil = new CreateTestConnectioInformationPortUtil();

    /**
     * Default constructor.
     */
    CreateTestEnvironmentNodeModel() {
        super(CreateTestDatabaseConnectionPortUtil.PORT_TYPE, CreateTestConnectioInformationPortUtil.PORT_TYPE);
    }

    @Override
    protected CreateTestPortUtil getDatabasePortUtil() {
        return m_dbPortUtil;
    }

    @Override
    protected CreateTestPortUtil getFileSystemPortUtil() {
        return m_fsPortUtil;
    }
}
