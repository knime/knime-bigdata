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
 *   May 16, 2024 (Bjoern Lohrmann, KNIME GmbH): created
 */
package org.knime.bigdata.databricks.workspace.connector;

import java.io.IOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.time.Duration;

import org.knime.bigdata.databricks.credential.DatabricksAccessTokenCredential;
import org.knime.bigdata.databricks.credential.DatabricksUsernamePasswordCredential;
import org.knime.bigdata.databricks.credential.DatabricksWorkspaceAccessor;
import org.knime.bigdata.databricks.rest.DatabricksRESTClient;
import org.knime.bigdata.databricks.rest.scim.ScimAPI;
import org.knime.bigdata.databricks.rest.scim.ScimUser;
import org.knime.bigdata.databricks.workspace.connector.WorkspaceConnectorSettings.AuthType;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.KNIMEException;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.message.Message;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.credentials.base.Credential;
import org.knime.credentials.base.CredentialPortObject;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.credentials.base.node.AuthenticatorNodeModel;
import org.knime.credentials.base.oauth.api.AccessTokenAccessor;

/**
 * The Databricks Workspace Connector node model.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SuppressWarnings("restriction")
public class WorkspaceConnectorNodeModel extends AuthenticatorNodeModel<WorkspaceConnectorSettings> {

    private static final ScimUser DUMMY_USER = new ScimUser();

    /**
     * @param portsConfig The node configuration.
     */
    protected WorkspaceConnectorNodeModel(final PortsConfiguration portsConfig) {
        super(portsConfig.getInputPorts(), portsConfig.getOutputPorts(), WorkspaceConnectorSettings.class);
    }

    @Override
    protected void validateOnConfigure(final PortObjectSpec[] inSpecs, final WorkspaceConnectorSettings settings)
        throws InvalidSettingsException {

        settings.validate(inSpecs);

        if (inSpecs != null && inSpecs.length > 0 && inSpecs[0] != null) {
            final CredentialPortObjectSpec credSpec = (CredentialPortObjectSpec)inSpecs[0];
            if (credSpec.getCredential(Credential.class).isPresent()) {
                try {
                    credSpec.toAccessor(AccessTokenAccessor.class);
                } catch (NoSuchCredentialException ex) {
                    throw new InvalidSettingsException(ex.getMessage(), ex);
                }
            }
        }
    }

    @Override
    protected Credential createCredential(final PortObject[] inObjects, final ExecutionContext exec,
        final WorkspaceConnectorSettings settings) throws Exception {

        AccessTokenAccessor ingoingAccessToken = null;
        if (inObjects != null && inObjects.length > 0 && inObjects[0] != null) {
            final CredentialPortObject ingoingCred = (CredentialPortObject)inObjects[0];
            ingoingAccessToken = ingoingCred.getSpec().toAccessor(AccessTokenAccessor.class);
        }

        final ScimUser scimUser;
        try {
            scimUser = getCurrentDatabricksUser(settings, ingoingAccessToken);
        } catch (AccessDeniedException e) {
            throw KNIMEException.of(Message.fromSummary("Authentication failed. Please provide valid credentials."), e);
        } catch (Exception e) { // NOSONAR intentional
            throw KNIMEException.of(Message.fromSummary("Failure while validating credentials: " + e.getMessage()), e);
        }

        return createCredentialInternal(settings, scimUser, ingoingAccessToken);
    }

    private static Credential createCredentialInternal(final WorkspaceConnectorSettings settings,
        final ScimUser scimUser, final AccessTokenAccessor maybeAccessToken) {

        final Credential credential;

        if (maybeAccessToken != null) {
            credential = new DatabricksAccessTokenCredential(//
                URI.create(settings.m_workspaceUrl), //
                maybeAccessToken, //
                scimUser.id, //
                scimUser.displayName);
        } else if (settings.m_authType == AuthType.USERNAME_PASSWORD) {
            credential = new DatabricksUsernamePasswordCredential(//
                URI.create(settings.m_workspaceUrl), //
                settings.m_usernamePassword.getUsername(), //
                settings.m_usernamePassword.getPassword(), //
                scimUser.id, //
                scimUser.displayName);
        } else if (settings.m_authType == AuthType.TOKEN) {
            credential = new DatabricksAccessTokenCredential(//
                URI.create(settings.m_workspaceUrl), //
                settings.m_token.getPassword(), //
                scimUser.id, //
                scimUser.displayName);
        } else {
            throw new IllegalArgumentException("Usupported auth type: " + settings.m_authType);
        }

        return credential;
    }

    private static ScimUser getCurrentDatabricksUser(final WorkspaceConnectorSettings settings,
        final AccessTokenAccessor maybeAccessToken) throws IOException {

        final DatabricksWorkspaceAccessor dummyAccessor =
            (DatabricksWorkspaceAccessor)createCredentialInternal(settings, DUMMY_USER, maybeAccessToken);

        final ScimAPI scimApi = DatabricksRESTClient.create(dummyAccessor, //
            ScimAPI.class, //
            Duration.ofSeconds(40), //
            Duration.ofSeconds(60));

        return scimApi.currentUser();
    }
}
