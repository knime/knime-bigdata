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
 *   Created on 29.05.2019 by Mareike
 */
package org.knime.bigdata.spark.local.node.create;

import java.util.Collection;
import java.util.Map;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.bigdata.spark.local.node.create.utils.CreateConnectionInformationPortUtil;
import org.knime.bigdata.spark.local.node.create.utils.CreateDBSessionPortUtil;
import org.knime.bigdata.spark.local.node.create.utils.CreateLocalBDEPortUtil;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.ICredentials;
import org.knime.core.node.workflow.VariableType;
import org.knime.database.VariableContext;
import org.knime.database.port.DBSessionPortObject;

/**
 * Node model for the "Create Local Big Data Environment" node using a Hive {@link DBSessionPortObject} and a file system
 * {@link ConnectionInformationPortObject} output port.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class LocalEnvironmentCreatorNodeModel2 extends AbstractLocalEnvironmentCreatorNodeModel {

    private class NodeModelVariableContext implements VariableContext {

        @Override
        public ICredentials getCredentials(final String id) {
            return getCredentialsProvider().get(id);
        }

        @Override
        public Collection<String> getCredentialsIds() {
            return getCredentialsProvider().listNames();
        }

        @Override
        public Map<String, FlowVariable> getInputFlowVariables() {
            return getAvailableInputFlowVariables();
        }

        @Override
        public Map<String, FlowVariable> getInputFlowVariables(final VariableType<?>[] types) {
            return getAvailableFlowVariables(types);
        }

    }

    private final VariableContext m_variableContext = new NodeModelVariableContext();

    private final CreateDBSessionPortUtil m_dbPortUtil = new CreateDBSessionPortUtil(this, m_variableContext);

    private final CreateConnectionInformationPortUtil m_fsPortUtil = new CreateConnectionInformationPortUtil();

    /**
     * Constructor.
     */
    LocalEnvironmentCreatorNodeModel2() {
        super(CreateDBSessionPortUtil.PORT_TYPE, CreateConnectionInformationPortUtil.PORT_TYPE, false);
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
