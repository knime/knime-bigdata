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
 *   2024-01-25 (bjoern): created
 */
package org.knime.bigdata.databricks.credential;

import static org.knime.credentials.base.secretstore.ParserUtil.getStringField;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.function.Supplier;

import org.knime.bigdata.databricks.rest.DatabricksRESTClient;
import org.knime.bigdata.databricks.rest.scim.ScimAPI;
import org.knime.bigdata.databricks.rest.scim.ScimUser;
import org.knime.core.node.NodeLogger;
import org.knime.credentials.base.oauth.api.AccessTokenCredential;
import org.knime.credentials.base.secretstore.SecretConsumableParserProvider;
import org.knime.credentials.base.secretstore.UnparseableSecretConsumableException;

import jakarta.json.JsonObject;
import jakarta.ws.rs.NotFoundException;

/**
 * {@link SecretConsumableParserProvider} that can parse a secret consumable (from KNIME Hub Secret Store) into a
 * {@link DatabricksAccessTokenCredential}.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 * @since 5.3.2
 */
public class DatabricksAccessTokenCredentialParserProvider
    extends SecretConsumableParserProvider<DatabricksAccessTokenCredential> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DatabricksAccessTokenCredentialParserProvider.class);

    /**
     * Zero-argument default constructor.
     */
    public DatabricksAccessTokenCredentialParserProvider() {
        super("databricks_oauth2", DatabricksAccessTokenCredentialParserProvider::parse);
        m_parsers.put("databricks_personal_access_token", DatabricksAccessTokenCredentialParserProvider::parse);
    }

    private static DatabricksAccessTokenCredential parse(final Supplier<JsonObject> consumableSupplier)
        throws UnparseableSecretConsumableException, IOException {

        final JsonObject consumable = fetchConsumable(consumableSupplier);
        final AccessTokenCredential accessTokenCredential = parseAccessTokenCredential(consumable, consumableSupplier);

        final DatabricksAccessTokenCredential naUserCredentials = new DatabricksAccessTokenCredential(//
            URI.create(getStringField(consumable, "workspaceUrl")), //
            accessTokenCredential, "NA", "NA");

        //try to fetch the current user info from Databricks
        String userId = null;
        String userName = null;
        try {
            final ScimAPI scimApi = DatabricksRESTClient.create(naUserCredentials, //
                ScimAPI.class, //
                Duration.ofSeconds(40), //
                Duration.ofSeconds(60));
            final ScimUser user = scimApi.currentUser();
            userId = user.id;
            userName = user.displayName;
        } catch (NotFoundException ex) { // NOSONAR this is a valid case
            //this happens for service principals and can be ignored -> use default id and name
        } catch (Exception ex) { // NOSONAR too many possibilities
            //another exception happens log warning but continue to use the default id and name
            LOGGER.warn("Unable to fetch current Databricks user using secret store secret", ex);
        }

        return new DatabricksAccessTokenCredential(//
            URI.create(getStringField(consumable, "workspaceUrl")), //
            accessTokenCredential, //
            userId, //
            userName);
    }

    private static JsonObject fetchConsumable(final Supplier<JsonObject> consumableSupplier) throws IOException {
        try {
            return consumableSupplier.get();
        } catch (UncheckedIOException e) { // NOSONAR this is just unwrapping
            throw e.getCause();
        }
    }

    private static AccessTokenCredential parseAccessTokenCredential(final JsonObject consumable,
        final Supplier<JsonObject> consumableSupplier)
        throws UnparseableSecretConsumableException {

        final Instant expires;
        final Supplier<AccessTokenCredential> tokenRefresher;
        //check if we have an expiresAt field or not which depends on the secret type
        if (consumable.containsKey("expiresAt")) {
            //this is for the databricks_oauth2 types which have an expiration
            expires = OffsetDateTime.parse(getStringField(consumable, "expiresAt")).toInstant();
            tokenRefresher = createTokenRefresher(consumableSupplier);
        } else {
            //this is the databricks_personal_access_token type which has no expiration
            expires = null;
            tokenRefresher = null;
        }

        return new AccessTokenCredential(//
            getStringField(consumable, "accessToken"), //
            expires,
            getStringField(consumable, "tokenType"), //
            tokenRefresher);//
    }

    /**
     * Create a new refresher, that consumes the secret from secret store again to refresh the secret. Any exceptions
     * are thrown in the process are wrapped into a {@link UncheckedIOException}, as per the API contract of
     * {@link AccessTokenCredential}.
     */
    private static Supplier<AccessTokenCredential> createTokenRefresher(final Supplier<JsonObject> consumableSupplier) {
        return () -> {
            try {
                final JsonObject consumable = fetchConsumable(consumableSupplier);
                return parseAccessTokenCredential(consumable, consumableSupplier);
            } catch (final UnparseableSecretConsumableException ex) {
                throw new UncheckedIOException(ex.getMessage(), new IOException(ex));
            } catch (IOException ex) {
                throw new UncheckedIOException(ex.getMessage(), ex);
            }
        };
    }
}
