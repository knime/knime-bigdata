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
package org.knime.bigdata.hive.testing;

import java.util.Map;

import org.knime.bigdata.commons.testing.TestflowVariable;
import org.knime.bigdata.hive.node.connector.HiveConnectorNodeModel;
import org.knime.bigdata.hive.node.connector.HiveConnectorSettings;
import org.knime.bigdata.hive.utility.HiveDriverDetector;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.workflow.FlowVariable;

/**
 * Provides factory methods to create {@link DatabaseConnectionSettings} objects for testing purposes.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @noreference This is testing code and its API is subject to change without notice.
 */
@Deprecated
public class TestingDatabaseConnectionSettingsFactory {

    /**
     * Creates a {@link DatabaseConnectionSettings} for a Hive connection with using settings from the given map of flow
     * variables.
     *
     * @param flowVars A map of flow variables that provide the connection settings.
     * @return a {@link DatabaseConnectionSettings}.
     */
    public static DatabaseConnectionSettings createHiveSettings(final Map<String, FlowVariable> flowVars) {
        final HiveConnectorSettings settings = new HiveConnectorSettings();

        settings.setDriver(HiveDriverDetector.getDriverName());
        settings.setHost(TestflowVariable.getString(TestflowVariable.HOSTNAME, flowVars));
        settings.setPort(TestflowVariable.getInt(TestflowVariable.HIVE_PORT, flowVars));
        settings.setParameter(TestflowVariable.getString(TestflowVariable.HIVE_PARAMETER, flowVars));
        settings.setDatabaseName(TestflowVariable.getString(TestflowVariable.HIVE_DATABASENAME, flowVars));

        final boolean useKerberos = TestflowVariable.isTrue(TestflowVariable.HIVE_USE_KERBEROS, flowVars);
        if (useKerberos) {
            settings.setKerberos(true);
        } else {
            settings.setUserName(TestflowVariable.getString(TestflowVariable.HIVE_USERNAME, flowVars));
            settings.setPassword(TestflowVariable.getString(TestflowVariable.HIVE_PASSWORD, flowVars));
        }

        settings.setJDBCUrl(HiveConnectorNodeModel.getJDBCURL(settings));

        return settings;
    }
}
