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
package org.knime.bigdata.spark.core.databricks;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.bigdata.databricks.DatabricksPlugin;
import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.core.context.SparkContextProvider;
import org.knime.bigdata.spark.core.databricks.context.DatabricksSparkContext;
import org.knime.bigdata.spark.core.databricks.context.DatabricksSparkContextConfig;
import org.knime.bigdata.spark.core.databricks.node.create.DatabricksSparkContextCreatorNodeSettings;
import org.knime.bigdata.spark.core.version.CompatibilityChecker;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.FlowVariable;

/**
 * Spark context provider that provides Databricks connectivity.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class DatabricksSparkContextProvider implements SparkContextProvider<DatabricksSparkContextConfig> {

    /**
     * The highest Spark version currently supported by the Databricks integration.
     */
    public static final SparkVersion HIGHEST_SUPPORTED_SPARK_VERSION;

    static {
        SparkVersion currHighest = DatabricksPlugin.DATABRICKS_SPARK_VERSION_CHECKER.getSupportedSparkVersions().iterator().next();
        for (SparkVersion curr : DatabricksPlugin.DATABRICKS_SPARK_VERSION_CHECKER.getSupportedSparkVersions()) {
            if (currHighest.compareTo(curr) < 0) {
                currHighest = curr;
            }
        }
        HIGHEST_SUPPORTED_SPARK_VERSION = currHighest;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompatibilityChecker getChecker() {
        return DatabricksPlugin.DATABRICKS_SPARK_VERSION_CHECKER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkVersion getHighestSupportedSparkVersion() {
        return HIGHEST_SUPPORTED_SPARK_VERSION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportSpark(final SparkVersion sparkVersion) {
        return getChecker().supportSpark(sparkVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<SparkVersion> getSupportedSparkVersions() {
        return getChecker().getSupportedSparkVersions();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkContext<DatabricksSparkContextConfig> createContext(final SparkContextID contextID) {
        return new DatabricksSparkContext(contextID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkContextIDScheme getSupportedScheme() {
        return SparkContextIDScheme.SPARK_DATABRICKS;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toPrettyString(final SparkContextID contextID) {
        if (contextID.getScheme() != SparkContextIDScheme.SPARK_DATABRICKS) {
            throw new IllegalArgumentException("Unsupported scheme: " + contextID.getScheme());
        }

        final URI uri = contextID.asURI();
        return String.format("%s on Databricks cluster at %s:%d", uri.getFragment(), uri.getHost(), uri.getPort());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<SparkContext<DatabricksSparkContextConfig>> createDefaultSparkContextIfPossible() {
        // currently, the Databricks connector never provides the default Spark context.
        return Optional.empty();
    }

    @Override
    public DatabricksSparkContextConfig createTestingSparkContextConfig(final Map<String, FlowVariable> flowVariables)
        throws InvalidSettingsException {

        final CredentialsProvider cp = null;
        final DatabricksSparkContextCreatorNodeSettings settings =
            DatabricksSparkContextCreatorNodeSettings.fromFlowVariables(flowVariables);
        final SparkContextID contextId = DatabricksSparkContextCreatorNodeSettings.createSparkContextID("testflowContext");
        final ConnectionInformation remoteFS = settings.createDBFSConnectionInformation(cp);
        return settings.createContextConfig(contextId, remoteFS, cp);
    }
}
