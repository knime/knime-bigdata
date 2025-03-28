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
package org.knime.bigdata.spark.core.databricks.node.create;

import java.util.Optional;

import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import org.knime.core.node.ConfigurableNodeFactory;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;
import org.knime.core.node.context.NodeCreationConfiguration;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.credentials.base.CredentialPortObject;
import org.knime.database.port.DBSessionPortObject;
import org.knime.filehandling.core.port.FileSystemPortObject;

/**
 * Databricks connector node factory.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DatabricksSparkContextCreatorNodeFactory2
    extends ConfigurableNodeFactory<DatabricksSparkContextCreatorNodeModel2> {

    private static final String WORKSPACE_INPUT_NAME = "Databricks Workspace Connection";

    private static final String DB_OUTPUT_NAME = "DB Connection";

    private static final String DBFS_OUTPUT_NAME = "DBFS Connection";

    private static final String SPARK_OUTPUT_NAME = "Spark Context";

    /**
     * Constructor.
     */
    public DatabricksSparkContextCreatorNodeFactory2() {
        super();
    }

    @Override
    protected Optional<PortsConfigurationBuilder> createPortsConfigBuilder() {
        final PortsConfigurationBuilder b = new PortsConfigurationBuilder();
        b.addOptionalInputPortGroup(WORKSPACE_INPUT_NAME, CredentialPortObject.TYPE);
        b.addFixedOutputPortGroup(DB_OUTPUT_NAME, DBSessionPortObject.TYPE);
        b.addFixedOutputPortGroup(DBFS_OUTPUT_NAME, FileSystemPortObject.TYPE);
        b.addFixedOutputPortGroup(SPARK_OUTPUT_NAME, SparkContextPortObject.TYPE);
        return Optional.of(b);
    }

    @Override
    protected DatabricksSparkContextCreatorNodeModel2 createNodeModel(final NodeCreationConfiguration creationConfig) {
        final PortsConfiguration portsConfig = creationConfig.getPortConfig().orElseThrow();
        final boolean useWorkspaceConnection = portsConfig.getInputPorts().length > 0;
        return new DatabricksSparkContextCreatorNodeModel2(portsConfig, useWorkspaceConnection);
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    protected NodeDialogPane createNodeDialogPane(final NodeCreationConfiguration creationConfig) {
        final PortsConfiguration portsConfig = creationConfig.getPortConfig().orElseThrow();
        final boolean useWorkspaceConnection = portsConfig.getInputPorts().length > 0;
        return new DatabricksSparkContextCreatorNodeDialog2(useWorkspaceConnection);
    }

    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    @Override
    public NodeView<DatabricksSparkContextCreatorNodeModel2> createNodeView(final int viewIndex,
        final DatabricksSparkContextCreatorNodeModel2 nodeModel) {
        return null;
    }

}
