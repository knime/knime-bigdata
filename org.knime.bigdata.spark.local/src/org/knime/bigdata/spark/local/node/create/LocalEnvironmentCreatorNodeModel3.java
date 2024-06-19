/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 */
package org.knime.bigdata.spark.local.node.create;

import java.util.Collection;
import java.util.Map;

import org.knime.bigdata.spark.local.node.create.utils.CreateDBSessionPortUtil;
import org.knime.bigdata.spark.local.node.create.utils.CreateFileSystemConnectionPortUtil;
import org.knime.bigdata.spark.local.node.create.utils.CreateLocalBDEPortUtil;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.ICredentials;
import org.knime.core.node.workflow.VariableType;
import org.knime.database.VariableContext;
import org.knime.database.port.DBSessionPortObject;
import org.knime.filehandling.core.port.FileSystemPortObject;

/**
 * Node model for the "Create Local Big Data Environment" node using a Hive {@link DBSessionPortObject} and a file system
 * {@link FileSystemPortObject} output port.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class LocalEnvironmentCreatorNodeModel3 extends AbstractLocalEnvironmentCreatorNodeModel {

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

    private final CreateFileSystemConnectionPortUtil m_fsPortUtil = new CreateFileSystemConnectionPortUtil(m_settings);

	LocalEnvironmentCreatorNodeModel3() {
		super(CreateDBSessionPortUtil.PORT_TYPE, CreateFileSystemConnectionPortUtil.PORT_TYPE, true);
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
