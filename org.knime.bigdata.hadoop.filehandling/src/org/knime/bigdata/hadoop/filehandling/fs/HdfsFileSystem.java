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
package org.knime.bigdata.hadoop.filehandling.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.knime.bigdata.commons.config.CommonConfigContainer;
import org.knime.bigdata.commons.hadoop.ConfigurationFactory;
import org.knime.bigdata.commons.hadoop.UserGroupUtil;
import org.knime.bigdata.commons.hadoop.UserGroupUtil.UserGroupInformationCallback;
import org.knime.bigdata.hadoop.filehandling.node.HdfsConnectorNodeSettings;
import org.knime.core.node.NodeLogger;
import org.knime.filehandling.core.connections.DefaultFSLocationSpec;
import org.knime.filehandling.core.connections.FSCategory;
import org.knime.filehandling.core.connections.FSFileSystem;
import org.knime.filehandling.core.connections.FSLocationSpec;
import org.knime.filehandling.core.connections.base.BaseFileSystem;
import org.knime.filehandling.core.defaultnodesettings.ExceptionUtil;

/**
 * HDFS implementation of the {@link FSFileSystem}.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class HdfsFileSystem extends BaseFileSystem<HdfsPath> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(HdfsFileSystem.class);

    /**
     * URI scheme of this {@link FSFileSystem}.
     */
    public static final String FS_TYPE = "hdfs";

    /**
     * Character to use as path separator
     */
    public static final String PATH_SEPARATOR = "/";

    private final FileSystem m_hadoopFileSystem;

    /**
     * Default constructor.
     *
     * @param cacheTTL The time to live for cached elements in milliseconds.
     * @param settings Connection settings.
     * @throws IOException
     */
    public HdfsFileSystem(final long cacheTTL, final HdfsConnectorNodeSettings settings) throws IOException {
        super(new HdfsFileSystemProvider(), //
            createURI(settings), //
            cacheTTL, //
            settings.getWorkingDirectory(), //
            createFSLocationSpec(settings.getHost()));

        final URI fsURI = settings.getHadopURI();
        final Configuration hadoopConf = createHadoopConfiguration(settings, fsURI);
        m_hadoopFileSystem = openHadoopFileSystem(settings, hadoopConf, fsURI);
    }

    /**
     * Non public constructor to create an instance using an existing Hadoop file system in integration tests.
     **/
    HdfsFileSystem(final long cacheTTL, final URI uri, final String host, final String workingDirectory,
        final FileSystem hadoopFileSystem) {

        super(new HdfsFileSystemProvider(), //
            uri, //
            cacheTTL, //
            workingDirectory, //
            createFSLocationSpec(host));

        m_hadoopFileSystem = hadoopFileSystem;
    }

    /**
     * @param settings settings.
     * @return URI from settings.
     * @throws URISyntaxException
     */
    private static URI createURI(final HdfsConnectorNodeSettings settings) throws IOException {
        try {
            return new URI(FS_TYPE, null, settings.getHost(), settings.getPort(), null, null, null);
        } catch (final URISyntaxException e) {
            throw new IOException("Failed to create file system URI: " + e.getMessage(), e);
        }
    }

    @SuppressWarnings("resource")
    private static Configuration createHadoopConfiguration(final HdfsConnectorNodeSettings settings, final URI uri) {
        final CommonConfigContainer commonConfig = CommonConfigContainer.getInstance();
        final Configuration config;

        if (settings.useKerberos()) {
            config = ConfigurationFactory.createBaseConfigurationWithKerberosAuth();
        } else {
            config = ConfigurationFactory.createBaseConfigurationWithSimpleAuth();
        }

        // set the default file system
        FileSystem.setDefaultUri(config, uri);

        // Disable file system caching inside Hadoop class (already cached in the instance of this class)
        config.setBoolean(String.format("fs.%s.impl.disable.cache", uri.getScheme()), true);

        // add core-site from preferences
        if (commonConfig.hasCoreSiteConfig()) {
            LOGGER.debug("Applying core-site.xml from KNIME Hadoop preferences to HDFS connection");
            config.addResource(commonConfig.getCoreSiteConfig());
        }

        // add hdfs-site from preferences
        if (commonConfig.hasHdfsSiteConfig()) {
            LOGGER.debug("Applying hdfs-site.xml from KNIME Hadoop preferences to HDFS connection");
            config.addResource(commonConfig.getHdfsSiteConfig());
        }

        return config;
    }

    private static FileSystem openHadoopFileSystem(final HdfsConnectorNodeSettings settings, final Configuration config,
        final URI uri) throws IOException {

        final UserGroupInformationCallback<FileSystem> fsCallback =
            ugi -> ugi.doAs((PrivilegedExceptionAction<FileSystem>)() -> FileSystem.get(uri, config)); // NOSONAR cast required

        try {
            if (settings.useKerberos()) {
                return UserGroupUtil.runWithProxyUserUGIIfNecessary(fsCallback);
            } else {
                return UserGroupUtil.runWithRemoteUserUGI(settings.getUser(), fsCallback);
            }
        } catch (Exception e) {
            throw ExceptionUtil.wrapAsIOException(e);
        }
    }

    /**
     * @param host HDFS name node or WebHDFS/HTTPFS endpoint
     * @return the {@link FSLocationSpec} for a HDFS file system.
     */
    public static DefaultFSLocationSpec createFSLocationSpec(final String host) {
        return new DefaultFSLocationSpec(FSCategory.CONNECTED, HdfsFileSystem.FS_TYPE + ":" + host);
    }

    /**
     * @return the wrapped Hadoop file system
     */
    public FileSystem getHadoopFileSystem() {
        return m_hadoopFileSystem;
    }

    @Override
    public HdfsPath getPath(final String first, final String... more) {
        return new HdfsPath(this, first, more);
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
        m_hadoopFileSystem.close();
    }
}
