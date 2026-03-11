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
package org.knime.bigdata.databricks.sparksql.connector;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.database.databricks.Databricks;
import org.knime.bigdata.databricks.node.ClusterChoiceProvider;
import org.knime.bigdata.databricks.sparksql.connector.SparkSQLConnectorParameters.ConnectorModification;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Modification;
import org.knime.database.node.connector.DBConnectorNodeSettingsUtils.DBDialectChoicesProvider;
import org.knime.database.node.connector.DBConnectorNodeSettingsUtils.DBDriverChoicesProvider;
import org.knime.database.node.connector.SpecificDBConnectorNodeSettings;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.widget.choices.ChoicesProvider;

/**
 * Node parameters for the Databricks Spark SQL Connector.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SuppressWarnings("restriction")
@Modification(ConnectorModification.class)
public class SparkSQLConnectorParameters extends SpecificDBConnectorNodeSettings {

    SparkSQLConnectorParameters() {
        super(Databricks.DB_TYPE);
    }

    static final class ConnectorModification extends ChoicesProviderModification {

        @Override
        protected Class<? extends DBDialectChoicesProvider> getDialectProvider() {
            return DatabricksDBDialectChoicesProvider.class;
        }

        @Override
        protected Class<? extends DBDriverChoicesProvider> getDriverProvider() {
            return DatabricksDBDriverChoicesProvider.class;
        }

        @Override
        protected String getJDBCDescription() {
            return "This tab allows you to define JDBC driver connection parameter. The value of a parameter can be a "
                + "constant, variable, credential user, credential password or KNIME URL. "
                + "The UserAgentEntry parameter is added as default to all Databricks connections to track the "
                + "usage of KNIME Analytics Platform as Databricks client. If you are not comfortable sharing this "
                + "information with Databricks you can remove the parameter. However, if you want to promote KNIME "
                + "as a client with Databricks leave the parameter as is. For more information about the JDBC "
                + "driver and the UserAgentEntry, refer to the installation and configuration guide which you can "
                + "find in the docs directory of the "
                + "<a href=\"https://www.databricks.com/spark/jdbc-drivers-download\">driver package.</a>";
        }
    }

    static final class DatabricksDBDialectChoicesProvider extends DBDialectChoicesProvider {
        DatabricksDBDialectChoicesProvider() {
            super(Databricks.DB_TYPE, true);
        }
    }

    static final class DatabricksDBDriverChoicesProvider extends DBDriverChoicesProvider {
        protected DatabricksDBDriverChoicesProvider() {
            super(Databricks.DB_TYPE);
        }
    }

    @Layout(ConnectionSection.class)
    @Widget( //
        title = "Cluster", //
        description = "Select the Databricks compute cluster to connect to. "
            + "The cluster must be running or will be started automatically when the node is executed." //
    )
    @ChoicesProvider(ClusterChoiceProvider.class)
    String m_clusterId = "";

    @Layout(ConnectionSection.class)
    @Widget( //
        title = "Terminate compute cluster on disconnect", //
        description = "If selected, the compute cluster will be terminated when the KNIME workflow is closed "
            + "or the node is reset. This can help reduce costs by ensuring that clusters are not "
            + "left running when they are no longer needed." //
    )
    @Persist(configKey = "terminateClusterOnDisconnect")
    boolean m_terminateClusterOnDisconnect;

    @Override
    public void validate() throws InvalidSettingsException {
        if (StringUtils.isAllBlank(m_clusterId)) {
            throw new InvalidSettingsException("Please select a Databricks cluster.");
        }
    }
}
