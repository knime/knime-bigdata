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

import java.sql.SQLException;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import org.knime.bigdata.spark.local.database.LocalHiveConnectionSettings;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabaseConnectionPortObject;
import org.knime.core.node.port.database.DatabaseConnectionPortObjectSpec;

/**
 * Node model for the "Create Local Big Data Environment" node.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class LocalEnvironmentCreatorNodeModel extends AbstractLocalEnvironmentCreatorNodeModel {

	/**
	 * Constructor.
	 */
	LocalEnvironmentCreatorNodeModel() {
		super(new PortType[]{}, new PortType[]{DatabaseConnectionPortObject.TYPE,
				ConnectionInformationPortObject.TYPE, SparkContextPortObject.TYPE});
	}

    @Override
    protected PortObject createDBPort(ExecutionContext exec, int hiveserverPort)
        throws CanceledExecutionException, SQLException, InvalidSettingsException {
        return new DatabaseConnectionPortObject(
            new DatabaseConnectionPortObjectSpec(new LocalHiveConnectionSettings(hiveserverPort)));
    }
}
