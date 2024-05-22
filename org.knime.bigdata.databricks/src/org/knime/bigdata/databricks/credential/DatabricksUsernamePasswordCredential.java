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
 *   2024-05-17 (bjoern): created
 */
package org.knime.bigdata.databricks.credential;

import static org.knime.credentials.base.CredentialPortViewUtil.obfuscate;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.cxf.common.util.Base64Utility;
import org.knime.credentials.base.Credential;
import org.knime.credentials.base.CredentialPortViewData;
import org.knime.credentials.base.CredentialPortViewData.Section;
import org.knime.credentials.base.CredentialType;
import org.knime.credentials.base.CredentialTypeRegistry;
import org.knime.credentials.base.NoOpCredentialSerializer;

/**
 * {@link Credential} that provides a Databricks username and password (for HTTP Basic authentication).
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class DatabricksUsernamePasswordCredential implements Credential, DatabricksWorkspaceAccessor {

    /**
     * The serializer class
     */
    public static class Serializer extends NoOpCredentialSerializer<DatabricksUsernamePasswordCredential> {
    }

    /**
     * Credential type.
     */
    public static final CredentialType TYPE =
        CredentialTypeRegistry.getCredentialType("knime.DatabricksUsernamePasswordCredential");

    private URI m_databricksWorkspaceUrl;

    private String m_username;

    private String m_password;

    private String m_userId;

    private String m_displayName;

    /**
     * Default constructor for ser(de).
     */
    public DatabricksUsernamePasswordCredential() {
    }

    /**
     * Constructor that wraps a given access token (OAuth2 or personal).
     *
     * @param databricksWorkspaceUrl The URL of the Databricks workspace to connect to.
     * @param username The Databricks username.
     * @param password The Databricks password.
     * @param userId The technical Databricks user id.
     * @param displayName The Databricks user display name.
     */
    public DatabricksUsernamePasswordCredential(final URI databricksWorkspaceUrl, final String username,
        final String password, final String userId, final String displayName) {

        m_databricksWorkspaceUrl = databricksWorkspaceUrl;
        m_username = username;
        m_password = password;
        m_userId = userId;
        m_displayName = displayName;
    }

    @Override
    public URI getDatabricksWorkspaceUrl() {
        return m_databricksWorkspaceUrl;
    }

    /**
     * @return the Databricks username
     */
    public String getUsername() {
        return m_username;
    }

    /**
     * @return the Databricks password
     */
    public String getPassword() {
        return m_password;
    }

    @Override
    public String getUserId() {
        return m_userId;
    }

    @Override
    public String getUserDisplayName() {
        return m_displayName;
    }

    @Override
    public String getAuthScheme() {
        return "Basic";
    }

    @Override
    public String getAuthParameters() throws IOException {
        return Base64Utility.encode((m_username + ":" + m_password).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public CredentialType getType() {
        return TYPE;
    }

    @Override
    public CredentialPortViewData describe() {
        return new CredentialPortViewData(List.of(new Section("Databricks Credentials", new String[][]{//
            {"Property", "Value"}, //
            {"Databricks workspace URL", m_databricksWorkspaceUrl.toString()}, //
            {"Username", m_username}, //
            {"Password", obfuscate(m_password)}, //
            {"Databricks user ID", m_userId}, //
            {"Databricks user", m_displayName}, //
        })));
    }
}
