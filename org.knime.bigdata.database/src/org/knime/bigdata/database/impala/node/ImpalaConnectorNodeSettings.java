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

import java.util.List;

import org.knime.bigdata.database.impala.Impala;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.webui.node.dialog.defaultdialog.setting.credentials.AuthenticationSettings;
import org.knime.core.webui.node.dialog.defaultdialog.setting.credentials.AuthenticationSettings.AuthenticationType;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Modification;
import org.knime.database.node.connector.DBConnectorNodeSettingsUtils.DBDialectChoicesProvider;
import org.knime.database.node.connector.DBConnectorNodeSettingsUtils.DBDriverChoicesProvider;
import org.knime.database.node.connector.server.UnauthenticatedServerDBConnectorNodeSettings;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.widget.choices.EnumChoicesProvider;
import org.knime.node.parameters.widget.credentials.Credentials;

/**
 * Node settings for the Impala Connector (WebUI).
 *
 * @author Halil Yerlikaya, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("restriction")
@Modification({ImpalaConnectorNodeSettings.ConnectorModification.class,
    ImpalaConnectorNodeSettings.DescriptionModification.class})
class ImpalaConnectorNodeSettings extends UnauthenticatedServerDBConnectorNodeSettings {

    private static final int DEFAULT_PORT = 21050;

    ImpalaConnectorNodeSettings() {
        super(Impala.DB_TYPE, DEFAULT_PORT);
    }

    static final class ConnectorModification extends ChoicesProviderModification {

        @Override
        protected Class<? extends DBDialectChoicesProvider> getDialectProvider() {
            return ImpalaDBDialectChoicesProvider.class;
        }

        @Override
        protected Class<? extends DBDriverChoicesProvider> getDriverProvider() {
            return ImpalaDBDriverChoicesProvider.class;
        }

        @Override
        protected String getJDBCDescription() {
            return """
                    This tab allows you to define driver-specific parameters for the JDBC connection. The value of a \
                    parameter can be a constant, credential user, credential password or KNIME URL.
                    For more information about the parameters supported by the included Apache Hive driver, please \
                    consult the \
                    <a href="https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#\
                    HiveServer2Clients-ConnectionURLs">Hive documentation</a>. When using a proprietary Hive driver, \
                    please consult the vendor documentation, e.g. \
                    <a href="https://www.cloudera.com/documentation/other/connectors/impala-jdbc/latest/\
                    Cloudera-JDBC-Driver-for-Impala-Install-Guide.pdf">Cloudera JDBC Driver for Apache Impala \
                    Install Guide</a>.
                    """;
        }
    }

    static final class ImpalaDBDialectChoicesProvider extends DBDialectChoicesProvider {

        ImpalaDBDialectChoicesProvider() {
            super(Impala.DB_TYPE, true);
        }
    }

    static final class ImpalaDBDriverChoicesProvider extends DBDriverChoicesProvider {

        protected ImpalaDBDriverChoicesProvider() {
            super(Impala.DB_TYPE);
        }
    }

    static final class DescriptionModification
        extends UnauthenticatedServerDBConnectorNodeSettings.ServerSettingsDescriptionModification {

        @Override
        protected String getDBTypeName() {
            return Impala.DB_TYPE.getName();
        }

        @Override
        protected int getDefaultPort() {
            return DEFAULT_PORT;
        }
    }

    @Layout(AuthenticationSection.class)
    @Modification(ChangeAvailableAuthenticationModification.class)
    AuthenticationSettings m_authentication =
        new AuthenticationSettings(AuthenticationType.USER_PWD, new Credentials());

    static final class ChangeAvailableAuthenticationModification extends DBAuthenticationTypeModification {

        @Override
        protected Class<? extends EnumChoicesProvider<AuthenticationType>> getAuthenticationTypeChoicesProvider() {
            return AvailableAuthenticationTypesProvider.class;
        }
    }

    static final class AvailableAuthenticationTypesProvider implements EnumChoicesProvider<AuthenticationType> {

        private static final List<AuthenticationType> TYPES =
            List.of(AuthenticationSettings.AuthenticationType.USER_PWD, //
                AuthenticationSettings.AuthenticationType.KERBEROS);

        @Override
        public List<AuthenticationType> choices(final NodeParametersInput context) {
            return TYPES;
        }
    }

    void validateAuthenticationSettings() throws InvalidSettingsException {
        final var authType = m_authentication.getType();
        final var validTypes = AvailableAuthenticationTypesProvider.TYPES;
        validateAuthType(authType, validTypes);
    }
}
