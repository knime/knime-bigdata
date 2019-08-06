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

import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.commons.testing.TestflowVariable;
import org.knime.bigdata.database.hive.HiveDriverLocator;
import org.knime.bigdata.database.hive.node.HiveConnectorSettings;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.database.attribute.Attribute;
import org.knime.database.attribute.Attribute.InvalidValueAction;
import org.knime.database.connection.DBConnectionManagerAttributes;
import org.knime.database.node.connector.server.ServerDBConnectorSettings;
import org.knime.database.util.DerivableProperties;
import org.knime.database.util.DerivableProperties.ValueType;

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
     * @throws InvalidSettingsException
     */
    public static ServerDBConnectorSettings createHiveSettings(final Map<String, FlowVariable> flowVars)
            throws InvalidSettingsException {
        final String hostname = TestflowVariable.getString(TestflowVariable.HOSTNAME, flowVars);
        final HiveConnectorSettings settings = new HiveConnectorSettings();

        settings.setDriver("hive");
        settings.setHost(hostname);
        settings.setPort(TestflowVariable.getInt(TestflowVariable.HIVE_PORT, flowVars));
        settings.setDatabaseName(TestflowVariable.getString(TestflowVariable.HIVE_DATABASENAME, flowVars));

        final String jdbcParams = TestflowVariable.getString(TestflowVariable.HIVE_PARAMETER, flowVars);
        if (!StringUtils.isBlank(jdbcParams)) {
            settings.transferAttributeValuesIn(getAttributes(jdbcParams));
        }

        final SettingsModelAuthentication authModel = settings.getAuthenticationModel();
        final boolean useKerberos = TestflowVariable.isTrue(TestflowVariable.HIVE_USE_KERBEROS, flowVars);
        if (useKerberos) {
            authModel.setValues(AuthenticationType.KERBEROS, null, null, null);
        } else {
            final String userName = TestflowVariable.getString(TestflowVariable.HIVE_USERNAME, flowVars);
            final String password = TestflowVariable.getString(TestflowVariable.HIVE_PASSWORD, flowVars);
            authModel.setValues(AuthenticationType.USER_PWD, null, userName, password);
        }

        return settings;
    }

    private static Collection<Attribute<?>> getAttributes(final String params) throws InvalidSettingsException {
        final Collection<Attribute<?>> editableAttributes = HiveDriverLocator.ATTRIBUTES.getEditableAttributes();
        @SuppressWarnings("unchecked")
        final Attribute<DerivableProperties> jdbcPropertiesAttribute = (Attribute<DerivableProperties>) editableAttributes.stream()
            .filter(a -> a.getId().equals(DBConnectionManagerAttributes.ATTRIBUTE_JDBC_PROPERTIES.getId()))
            .findAny()
            .orElseThrow(() -> new InvalidSettingsException("Unable to add JDBC properties to Hive connection settings."));

        final DerivableProperties jdbcProperties = new DerivableProperties();
        for (final String param : params.split(";")) {
            final String[] kv = param.split("=", 2);
            jdbcProperties.setDerivableProperty(kv[0], ValueType.LITERAL, kv[1]);
        }
        jdbcPropertiesAttribute.setValue(jdbcProperties, InvalidValueAction.THROW_EXCEPTION);

        return editableAttributes;
    }
}
