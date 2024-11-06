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
 *   Created on Jul 20, 2018 by bjoern
 */
package org.knime.bigdata.database.databricks.testing;

import static org.knime.bigdata.spark.core.databricks.node.create.ClusterDBControllerFactory.getHttpPath;

import java.net.URI;
import java.util.Map;

import org.apache.hive.jdbc.HiveDriver;
import org.knime.base.node.io.database.connection.util.DefaultDatabaseConnectionSettings;
import org.knime.bigdata.database.databricks.DatabricksUserDBConnectionController;
import org.knime.bigdata.spark.core.databricks.node.create.ClusterDBControllerFactory;
import org.knime.bigdata.spark.core.databricks.node.create.DatabricksSparkContextCreatorNodeSettings;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.database.connection.DBConnectionController;
import org.knime.database.node.connector.DBSessionSettings;

/**
 * Provides factory methods to create {@link DBSessionSettings} objects for testing purposes.
 *
 * @author Sascha Wolke, KNIME GmbH
 * @noreference This is testing code and its API is subject to change without notice.
 */
public class DatabricksTestingDatabaseConnectionSettingsFactory {

    /**
     * Creates {@link DBSessionSettings} from the given map of flow variables.
     *
     * @param flowVariables A map of flow variables that provide the Spark context settings.
     * @return {@link DBSessionSettings} of the remote cluster.
     * @throws InvalidSettingsException on invalid flow variables
     */
    public static DBSessionSettings createSettings(final Map<String, FlowVariable> flowVariables)
            throws InvalidSettingsException {
        return DatabricksSparkContextCreatorNodeSettings.fromFlowVariables(flowVariables);
    }

    /**
     * Creates {@link DBConnectionController} from the given map of flow variables.
     *
     * @param jdbcUrl JDBC URL to use
     * @param flowVariables A map of flow variables that provide the Spark context settings.
     * @return {@link DatabricksUserDBConnectionController}
     * @throws InvalidSettingsException on invalid flow variables
     */
    public static DBConnectionController createDBConnectionController(final String jdbcUrl,
            final Map<String, FlowVariable> flowVariables) throws InvalidSettingsException {
        final DatabricksSparkContextCreatorNodeSettings settings =
            DatabricksSparkContextCreatorNodeSettings.fromFlowVariables(flowVariables);
        final String username = settings.getUsername(null);
        final String password = settings.getPassword(null);
        return ClusterDBControllerFactory.create(jdbcUrl, null, settings.getClusterId(),
            settings.getWorkspaceId(), username, password);
    }

    /**
     * Creates deprecated {@link DatabaseConnectionSettings} from the given map of flow variables.
     *
     * @param flowVariables A map of flow variables that provide the Spark context settings.
     * @return {@link DatabaseConnectionSettings}
     * @throws InvalidSettingsException on invalid flow variables
     */
    public static DatabaseConnectionSettings createDeprecatedDBSettings(final Map<String, FlowVariable> flowVariables)
            throws InvalidSettingsException {

        final DatabricksSparkContextCreatorNodeSettings settings =
            DatabricksSparkContextCreatorNodeSettings.fromFlowVariables(flowVariables);
        final DefaultDatabaseConnectionSettings result = new DefaultDatabaseConnectionSettings();

        result.setDriver(HiveDriver.class.getName());
        result.setUserName(settings.getUsername(null));
        result.setPassword(settings.getPassword(null));
        final URI uri = URI.create(settings.getDatabricksInstanceURL());
        result.setHost(uri.getHost());
        result.setPort(uri.getPort() > 0 ? uri.getPort() : 443);
        result.setDatabaseName("default");

        final String httpPath = getHttpPath(settings.getClusterId(), settings.getWorkspaceId());
        result.setJDBCUrl(settings.getDBUrl() + ";transportMode=http;ssl=true;httpPath=" + httpPath);

        // from HiveConnectorSettings
        result.setRowIdsStartWithZero(true);
        result.setRetrieveMetadataInConfigure(false);
        result.setDatabaseIdentifier("hive2");

        return result;
    }
}
