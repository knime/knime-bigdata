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
 *
 * History
 *   Mar 5, 2026 (Halil Yerlikaya, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.database.impala.node;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.webui.node.dialog.defaultdialog.setting.credentials.AuthenticationSettings;
import org.knime.database.node.connector.SpecificDBConnectorNodeMigrationRule;
import org.knime.node.parameters.widget.credentials.Credentials;
import org.knime.workflow.migration.MigrationException;
import org.knime.workflow.migration.MigrationNodeMatchResult;
import org.knime.workflow.migration.NodeMigrationAction;
import org.knime.workflow.migration.model.MigrationNode;
import org.knime.workflow.migration.util.NodeSettingsMigrationVariablesUtilities;

/**
 * Node migration rule for migrating the <em>Impala Connector</em> node to the WebUI variant.
 *
 * @author Halil Yerlikaya, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("restriction")
public class ImpalaConnectorNodeMigrationRule2
    extends SpecificDBConnectorNodeMigrationRule<ImpalaConnectorNodeSettings, ImpalaConnectorSettings> {

    private static final String CONNECTION_KEY = "impala-connection";

    private static final String CONNECTION_HOST_KEY = "host";

    private static final String CONNECTION_PORT_KEY = "port";

    private static final String CONNECTION_DB_OLD_KEY = "database_name";

    private static final String CONNECTION_DB_NEW_KEY = "databaseName";

    @Override
    protected Class<? extends NodeFactory<?>> getReplacementNodeFactoryClass(final MigrationNode migrationNode,
        final MigrationNodeMatchResult matchResult) {
        return ImpalaConnectorNodeFactory2.class;
    }

    @Override
    protected MigrationNodeMatchResult match(final MigrationNode migrationNode) {
        return MigrationNodeMatchResult.of(migrationNode,
            ImpalaConnectorNodeFactory.class.getName().equals(migrationNode.getOriginalNodeFactoryClassName())
                ? NodeMigrationAction.REPLACE : null);
    }

    @Override
    protected ImpalaConnectorNodeSettings createNewSettings() {
        return new ImpalaConnectorNodeSettings();
    }

    @Override
    protected ImpalaConnectorSettings createOldSettings() {
        return new ImpalaConnectorSettings();
    }

    @Override
    protected void migrateConnectionInfo(final ImpalaConnectorNodeSettings newSettings,
        final ImpalaConnectorSettings oldSettings) throws InvalidSettingsException {
        super.migrateConnectionInfo(newSettings, oldSettings);
        newSettings.m_host = oldSettings.getHost();
        newSettings.m_port = oldSettings.getPort();
        newSettings.m_databaseName = oldSettings.getDatabaseName();
    }

    @Override
    protected void migrateAuthentication(final ImpalaConnectorNodeSettings newSettings,
        final ImpalaConnectorSettings oldSettings, final NodeSettingsWO variablesTree)
        throws InvalidSettingsException, MigrationException {
        final var authSettings = oldSettings.getAuthenticationModel();
        if (authSettings.getAuthenticationType() == AuthenticationType.CREDENTIALS) {
            newSettings.m_authentication =
                new AuthenticationSettings(AuthenticationSettings.AuthenticationType.USER_PWD, new Credentials());
            NodeSettingsMigrationVariablesUtilities.addFlowVariable(variablesTree.addConfig("authentication"),
                "credentials", authSettings.getCredential());
        } else if (authSettings.getAuthenticationType() != AuthenticationType.NONE
            && authSettings.getAuthenticationType() != AuthenticationType.USER
            && authSettings.getAuthenticationType() != AuthenticationType.USER_PWD
            && authSettings.getAuthenticationType() != AuthenticationType.KERBEROS) {
            throw new MigrationException(
                String.format("Cannot migrate settings since authentication type %s is no longer supported",
                    authSettings.getAuthenticationType()));
        } else {
            final var credentials = new Credentials(authSettings.getUsername(), authSettings.getPassword());
            final var authType = switch (authSettings.getAuthenticationType()) {
                case USER -> AuthenticationSettings.AuthenticationType.USER;
                case USER_PWD -> AuthenticationSettings.AuthenticationType.USER_PWD;
                case KERBEROS -> AuthenticationSettings.AuthenticationType.KERBEROS;
                default -> AuthenticationSettings.AuthenticationType.NONE;
            };
            newSettings.m_authentication = new AuthenticationSettings(authType, credentials);
        }
    }

    @Override
    protected void migrateFlowVariables(final NodeSettingsRO tree, final NodeSettingsWO variablesTree)
        throws InvalidSettingsException {
        super.migrateFlowVariables(tree, variablesTree);
        if (!tree.containsKey(CONNECTION_KEY)) {
            return;
        }
        final var connection = tree.getNodeSettings(CONNECTION_KEY);
        if (connection.containsKey(CONNECTION_HOST_KEY)) {
            NodeSettingsMigrationVariablesUtilities.copyFlowVariable(variablesTree, CONNECTION_HOST_KEY,
                connection.getConfig(CONNECTION_HOST_KEY));
        }
        if (connection.containsKey(CONNECTION_PORT_KEY)) {
            NodeSettingsMigrationVariablesUtilities.copyFlowVariable(variablesTree, CONNECTION_PORT_KEY,
                connection.getConfig(CONNECTION_PORT_KEY));
        }
        if (connection.containsKey(CONNECTION_DB_OLD_KEY)) {
            NodeSettingsMigrationVariablesUtilities.copyFlowVariable(variablesTree, CONNECTION_DB_NEW_KEY,
                connection.getConfig(CONNECTION_DB_OLD_KEY));
        }
    }
}
