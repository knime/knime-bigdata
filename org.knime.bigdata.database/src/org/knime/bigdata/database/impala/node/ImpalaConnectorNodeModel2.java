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

import org.knime.bigdata.database.impala.Impala;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.webui.node.dialog.defaultdialog.setting.credentials.AuthenticationSettings;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.database.connection.DBConnectionController;
import org.knime.database.connection.UserDBConnectionController;
import org.knime.database.node.connector.server.UnauthenticatedServerDBConnectorNodeModel2;

/**
 * Node model for the Impala Connector (WebUI).
 *
 * @author Halil Yerlikaya, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("restriction")
public class ImpalaConnectorNodeModel2
    extends UnauthenticatedServerDBConnectorNodeModel2<ImpalaConnectorNodeSettings> {

    /**
     * @param configuration the node configuration
     */
    protected ImpalaConnectorNodeModel2(final WebUINodeConfiguration configuration) {
        super(Impala.DB_TYPE, configuration, ImpalaConnectorNodeSettings.class);
    }

    @Override
    protected DBConnectionController createConnectionController(final ImpalaConnectorNodeSettings modelSettings)
        throws InvalidSettingsException {
        final var credentials = modelSettings.m_authentication.getCredentials();
        return new UserDBConnectionController(getDBUrl(modelSettings),
            getAuthenticationType(modelSettings.m_authentication), credentials.getUsername(), credentials.getPassword(),
            null, null, modelSettings.m_host);
    }

    private static AuthenticationType getAuthenticationType(final AuthenticationSettings authSettings) {
        return switch (authSettings.getType()) {
            case KERBEROS -> AuthenticationType.KERBEROS;
            case USER_PWD -> AuthenticationType.USER_PWD;
            default -> throw new IllegalArgumentException("Unknown Authentication type");
        };
    }

    @Override
    protected void validateSessionInfoSettings(final ImpalaConnectorNodeSettings modelSettings)
        throws InvalidSettingsException {
        modelSettings.validateAuthenticationSettings();
    }
}
