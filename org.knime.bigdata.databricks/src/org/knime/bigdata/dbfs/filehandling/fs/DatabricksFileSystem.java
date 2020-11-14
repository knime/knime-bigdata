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
 *   2020-10-14 (Alexander Bondaletov): created
 */
package org.knime.bigdata.dbfs.filehandling.fs;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Collections;

import org.knime.bigdata.databricks.rest.DatabricksRESTClient;
import org.knime.bigdata.databricks.rest.dbfs.DBFSAPI;
import org.knime.bigdata.dbfs.filehandling.node.DatabricksConnectorSettings;
import org.knime.bigdata.dbfs.filehandling.node.DbfsAuthenticationNodeSettings;
import org.knime.bigdata.dbfs.filehandling.node.DbfsAuthenticationNodeSettings.AuthType;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.ICredentials;
import org.knime.filehandling.core.connections.DefaultFSLocationSpec;
import org.knime.filehandling.core.connections.FSCategory;
import org.knime.filehandling.core.connections.FSLocationSpec;
import org.knime.filehandling.core.connections.base.BaseFileSystem;


/**
 * Databricks DBFS implementation of the {@link FileSystem}.
 *
 * @author Alexander Bondaletov
 */
public class DatabricksFileSystem extends BaseFileSystem<DatabricksPath> {

    /**
     * Character to use as path separator
     */
    public static final String PATH_SEPARATOR = "/";

    private final DBFSAPI m_client;

    /**
     * @param uri the URI for the file system
     * @param cacheTTL The time to live for cached elements in milliseconds.
     * @param settings The settings.
     * @param credentialsProvider The {@link CredentialsProvider}.
     * @throws IOException
     */
    protected DatabricksFileSystem(final URI uri, final long cacheTTL, final DatabricksConnectorSettings settings,
        final CredentialsProvider credentialsProvider)
            throws IOException {
        super(new DatabricksFileSystemProvider(), uri, cacheTTL, settings.getWorkingDirectory(),
            createFSLocationSpec(uri.getHost()));

        m_client = createClient(settings, credentialsProvider);
    }

    /**
     * @param deployment Databricks deployment host
     * @return the {@link FSLocationSpec} for a Databricks file system.
     */
    public static DefaultFSLocationSpec createFSLocationSpec(final String deployment) {
        return new DefaultFSLocationSpec(FSCategory.CONNECTED,
            String.format("%s:%s", DatabricksFileSystemProvider.FS_TYPE, deployment));
    }

    private static DBFSAPI createClient(final DatabricksConnectorSettings settings,
        final CredentialsProvider credentialsProvider) throws UnsupportedEncodingException {

        final DbfsAuthenticationNodeSettings authSettings = settings.getAuthenticationSettings();

        if (authSettings.getAuthType() == AuthType.TOKEN) {

            final String token = authSettings.useTokenCredentials() //
                ? getCredentials(authSettings.getTokenCredentialsName(), credentialsProvider).getPassword()
                : authSettings.getTokenModel().getStringValue();

            return DatabricksRESTClient.create(settings.getDeploymentUrl(), DBFSAPI.class, token,
                settings.getReadTimeout(), settings.getConnectionTimeout());
        } else {
            String username;
            String password;

            if (authSettings.useUserPassCredentials()) {
                final ICredentials creds =
                    getCredentials(authSettings.getUserPassCredentialsName(), credentialsProvider);
                username = creds.getLogin();
                password = creds.getPassword();
            } else {
                username = authSettings.getUserModel().getStringValue();
                password = authSettings.getPasswordModel().getStringValue();
            }

            return DatabricksRESTClient.create(settings.getDeploymentUrl(), DBFSAPI.class, username, password,
                settings.getReadTimeout(), settings.getConnectionTimeout());
        }
    }

    private static ICredentials getCredentials(final String name, final CredentialsProvider credProvider) {
        if (credProvider == null) {
            throw new IllegalStateException("Credential provider is not available");
        }
        return credProvider.get(name);
    }



    /**
     * @return the client
     */
    public DBFSAPI getClient() {
        return m_client;
    }

    @Override
    protected void prepareClose() throws IOException {
        if (m_client != null) {
            DatabricksRESTClient.close(m_client);
        }
    }

    @Override
    public DatabricksPath getPath(final String first, final String... more) {
        return new DatabricksPath(this, first, more);
    }

    @Override
    public String getSeparator() {
        return PATH_SEPARATOR;
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return Collections.singletonList(getPath(PATH_SEPARATOR));
    }

}
