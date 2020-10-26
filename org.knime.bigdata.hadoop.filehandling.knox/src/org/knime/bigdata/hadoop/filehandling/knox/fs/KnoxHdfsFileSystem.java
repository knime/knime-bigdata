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
 */
package org.knime.bigdata.hadoop.filehandling.knox.fs;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.knime.bigdata.filehandling.knox.KnoxHDFSClient;
import org.knime.bigdata.filehandling.knox.rest.WebHDFSAPI;
import org.knime.bigdata.hadoop.filehandling.knox.node.KnoxHdfsConnectorNodeSettings;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.filehandling.core.connections.DefaultFSLocationSpec;
import org.knime.filehandling.core.connections.FSCategory;
import org.knime.filehandling.core.connections.FSFileSystem;
import org.knime.filehandling.core.connections.FSLocationSpec;
import org.knime.filehandling.core.connections.base.BaseFileSystem;

/**
 * WebHDFS via KNOX implementation of the {@link FSFileSystem}.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class KnoxHdfsFileSystem extends BaseFileSystem<KnoxHdfsPath> {

    /**
     * URI scheme of this {@link FSFileSystem}.
     */
    public static final String FS_TYPE = "hdfs-knox";

    /**
     * Character to use as path separator
     */
    public static final String PATH_SEPARATOR = "/";

    private final String m_host;

    private final WebHDFSAPI m_client;

    private final ExecutorService m_uploadExecutor;

    /**
     * Default constructor.
     *
     * @param cacheTTL The time to live for cached elements in milliseconds.
     * @param settings Connection settings.
     * @param cp credentials provider to use, might be {@code null} if not required
     * @throws IOException
     */
    public KnoxHdfsFileSystem(final long cacheTTL, final KnoxHdfsConnectorNodeSettings settings,
        final CredentialsProvider cp) throws IOException {

        super(new KnoxHdfsFileSystemProvider(), //
            createURI(settings), //
            cacheTTL, //
            settings.getWorkingDirectory(), //
            createFSLocationSpec(settings.getHost()));

        try {
            m_host = settings.getHost();
            m_client = KnoxHDFSClient.createClientBasicAuth(
                settings.getURL(),
                settings.getUser(cp),
                settings.getPassword(cp),
                settings.getReceiveTimeout() * 1000,
                settings.getConnectionTimeout() * 1000);
            m_uploadExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "knox-webhdfs-uploader"));
        } catch (final URISyntaxException e) {
            throw new IOException(e);
        }
    }

    /**
     * @param settings settings.
     * @return URI from settings.
     * @throws URISyntaxException
     */
    private static URI createURI(final KnoxHdfsConnectorNodeSettings settings) throws IOException {
        try {
            final URI endpoint = URI.create(settings.getURL());
            return new URI(FS_TYPE, null, endpoint.getHost(), endpoint.getPort(), endpoint.getPath(), null, null);
        } catch (final URISyntaxException e) {
            throw new IOException("Failed to create file system URI: " + e.getMessage(), e);
        }
    }

    /**
     * @param hostname of the KNOX gateway
     * @return the {@link FSLocationSpec} for a KNOX file system.
     */
    public static DefaultFSLocationSpec createFSLocationSpec(final String hostname) {
        return new DefaultFSLocationSpec(FSCategory.CONNECTED, KnoxHdfsFileSystem.FS_TYPE + ":" + hostname);
    }

    WebHDFSAPI getClient() {
        return m_client;
    }

    OutputStream createFile(final String path, final boolean overwrite) throws IOException {
        return KnoxHDFSClient.createFile(m_client, m_uploadExecutor, path, overwrite);
    }

    OutputStream appendFile(final String path) throws IOException {
        return KnoxHDFSClient.appendFile(m_client, m_uploadExecutor, path);
    }

    @Override
    public String getSchemeString() {
        return FS_TYPE;
    }

    @Override
    public String getHostString() {
        return m_host;
    }

    @Override
    public KnoxHdfsPath getPath(final String first, final String... more) {
        return new KnoxHdfsPath(this, first, more);
    }

    @Override
    public String getSeparator() {
        return PATH_SEPARATOR;
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return Collections.singletonList(getPath(PATH_SEPARATOR));
    }

    @Override
    protected void prepareClose() throws IOException {
        m_uploadExecutor.shutdownNow();
    }
}
