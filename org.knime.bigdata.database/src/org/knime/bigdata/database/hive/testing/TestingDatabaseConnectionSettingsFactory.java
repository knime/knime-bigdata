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
package org.knime.bigdata.database.hive.testing;

import java.util.Map;

import org.knime.bigdata.commons.testing.TestflowVariable;
import org.knime.bigdata.database.hive.node.HiveConnectorSettings;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.database.node.connector.server.ServerDBConnectorSettings;

/**
 * Provides factory methods to create {@link ServerDBConnectorSettings} objects for testing purposes.
 *
 * @author Sascha Wolke, KNIME GmbH
 * @noreference This is testing code and its API is subject to change without notice.
 */
public class TestingDatabaseConnectionSettingsFactory {

    /**
     * Creates a {@link ServerDBConnectorSettings} for a Hive connection with using settings from the given map of flow
     * variables.
     *
     * @param flowVars A map of flow variables that provide the connection settings.
     * @return a {@link ServerDBConnectorSettings}.
     */
    public static ServerDBConnectorSettings createHiveSettings(final Map<String, FlowVariable> flowVars) {
        final String hostname = TestflowVariable.getString(TestflowVariable.HOSTNAME, flowVars);
        final HiveConnectorSettings settings = new HiveConnectorSettings();

        settings.setDriver("hive");
        settings.setHost(hostname);
        settings.setPort(TestflowVariable.getInt(TestflowVariable.HIVE_PORT, flowVars));
        settings.setDatabaseName(TestflowVariable.getString(TestflowVariable.HIVE_DATABASENAME, flowVars));

        final SettingsModelAuthentication authModel = settings.getAuthenticationModel();
        final boolean useKerberos = TestflowVariable.isTrue(TestflowVariable.HIVE_USE_KERBEROS, flowVars);
        if (useKerberos) {
            authModel.setValues(AuthenticationType.KERBEROS, null, null, null);
        } else {
            final String userName = TestflowVariable.getString(TestflowVariable.HIVE_USERNAME, flowVars);
            final String password = TestflowVariable.getString(TestflowVariable.HIVE_PASSWORD, flowVars);
            authModel.setValues(AuthenticationType.USER_PWD, null, userName, password);
        }

        final String parameter = TestflowVariable.getString(TestflowVariable.HIVE_PARAMETER, flowVars);
        settings.setDBUrl(String.format("jdbc:hive2://%s:%s/%s;%s",
            settings.getHost(), settings.getPort(), settings.getDatabaseName(), parameter));

        return settings;
    }
}
