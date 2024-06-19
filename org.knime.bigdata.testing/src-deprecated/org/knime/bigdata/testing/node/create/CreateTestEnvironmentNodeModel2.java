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
 */
package org.knime.bigdata.testing.node.create;

import java.util.Collection;
import java.util.Map;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.bigdata.testing.node.create.utils.CreateTestConnectioInformationPortUtil;
import org.knime.bigdata.testing.node.create.utils.CreateTestDBSessionPortUtil;
import org.knime.bigdata.testing.node.create.utils.CreateTestPortUtil;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.ICredentials;
import org.knime.core.node.workflow.VariableType;
import org.knime.database.VariableContext;
import org.knime.database.port.DBSessionPortObject;

/**
 * Node model for the "Create Big Data Test Environment" node using a Hive {@link DBSessionPortObject} and a file system
 * {@link ConnectionInformationPortObject} output port.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class CreateTestEnvironmentNodeModel2 extends AbstractCreateTestEnvironmentNodeModel {

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

    private final CreateTestDBSessionPortUtil m_dbPortUtil = new CreateTestDBSessionPortUtil(this, m_variableContext);

    private final CreateTestConnectioInformationPortUtil m_fsPortUtil = new CreateTestConnectioInformationPortUtil();

    /**
     * Default constructor.
     */
    CreateTestEnvironmentNodeModel2() {
        super(CreateTestDBSessionPortUtil.PORT_TYPE, CreateTestConnectioInformationPortUtil.PORT_TYPE);
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
