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
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.knime.credentials.base.Credential;
import org.knime.credentials.base.CredentialPortViewData;
import org.knime.credentials.base.CredentialPortViewData.Section;
import org.knime.credentials.base.CredentialType;
import org.knime.credentials.base.CredentialTypeRegistry;
import org.knime.credentials.base.NoOpCredentialSerializer;
import org.knime.credentials.base.oauth.api.AccessTokenAccessor;
import org.knime.credentials.base.oauth.api.AccessTokenCredential;
import org.knime.credentials.base.oauth.api.HttpAuthorizationHeaderCredentialValue;

/**
 * {@link Credential} that provides a Databricks access token (Bearer).
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class DatabricksAccessTokenCredential
    implements Credential, AccessTokenAccessor, HttpAuthorizationHeaderCredentialValue {

    private static final String TOKEN_TYPE_BEARER = "Bearer";

    /**
     * The serializer class
     */
    public static class Serializer extends NoOpCredentialSerializer<DatabricksAccessTokenCredential> {
    }

    /**
     * Credential type.
     */
    public static final CredentialType TYPE =
        CredentialTypeRegistry.getCredentialType("knime.DatabricksAccessTokenCredential");

    private URI m_databricksWorkspaceUrl;

    private AccessTokenAccessor m_wrappedAccessToken;

    private String m_userId;

    private String m_displayName;

    /**
     * Default constructor for ser(de).
     */
    public DatabricksAccessTokenCredential() {
    }

    /**
     * Constructor that wraps a given Databricks personal access token.
     *
     * @param databricksWorkspaceUrl The URL of the Databricks workspace to connect to.
     * @param accessToken The Databricks personal access token.
     * @param userId The technical Databricks user id. May be null in case of a service principal.
     * @param displayName The Databricks user display name. May be null in case of a service principal.
     */
    public DatabricksAccessTokenCredential(final URI databricksWorkspaceUrl, final String accessToken,
        final String userId, final String displayName) {
        this(databricksWorkspaceUrl,//
            new AccessTokenCredential(accessToken, null, TOKEN_TYPE_BEARER, null),//
            userId,//
            displayName);
    }

    /**
     * Constructor that wraps a given {@link AccessTokenAccessor}.
     *
     * @param databricksWorkspaceUrl The URL of the Databricks workspace to connect to.
     * @param accessTokenAccessor The Databricks personal access token.
     * @param userId The technical Databricks user id.
     * @param displayName The Databricks user display name.
     */
    public DatabricksAccessTokenCredential(final URI databricksWorkspaceUrl,
        final AccessTokenAccessor accessTokenAccessor, final String userId, final String displayName) {

        m_databricksWorkspaceUrl = databricksWorkspaceUrl;
        m_wrappedAccessToken = accessTokenAccessor;
        m_userId = userId;
        m_displayName = displayName;
    }

    /**
     * @return the Databricks workspace URL
     */
    public URI getDatabricksWorkspaceUrl() {
        return m_databricksWorkspaceUrl;
    }

    @Override
    public String getAccessToken() throws IOException {
        return m_wrappedAccessToken.getAccessToken();
    }

    @Override
    public String getAccessToken(final boolean forceRefresh) throws IOException {
        return m_wrappedAccessToken.getAccessToken(forceRefresh);
    }

    /**
     * @return the Databricks user ID, which is automatically provisioned by Databricks. The optional may be empty if a
     *         service principal is being used.
     */
    public Optional<String> getUserId() {
        return Optional.ofNullable(m_userId);
    }

    /**
     * @return Concatenation of given and family names for the user (only for display purposes). The optional may be
     *         empty if a service principal is being used.
     */
    public Optional<String> getUserDisplayName() {
        return Optional.ofNullable(m_displayName);
    }

    @Override
    public String getTokenType() {
        return m_wrappedAccessToken.getTokenType();
    }

    @Override
    public String getAuthScheme() {
        return getTokenType();
    }

    @Override
    public String getAuthParameters() throws IOException {
        return getAccessToken();
    }

    @Override
    public Optional<Instant> getExpiresAfter() {
        return Optional.empty();
    }

    @Override
    public Set<String> getScopes() {
        return Collections.emptySet();
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
            {"Databricks user ID", getUserId().orElse("n/a")}, //
            {"Databricks user", getUserDisplayName().orElse("n/a")}, //
            {"Token", obfuscate("xxxxxxxxxxxxxxxxxxxxxxxx")}, // okay, this is just for show really...
            {"Token type", getTokenType()} //
        })));
    }
}
