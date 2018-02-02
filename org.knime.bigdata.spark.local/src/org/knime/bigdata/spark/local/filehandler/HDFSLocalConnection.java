package org.knime.bigdata.spark.local.filehandler;

/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
+ *   Created on Jan 22, 2018 by oole
 */


import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.bigdata.commons.config.CommonConfigContainer;
import org.knime.core.node.NodeLogger;

/**
 *
 * @author Ole Ostergaard, KNIME GmbH, Konstanz, Germany
 */
public class HDFSLocalConnection extends Connection {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(HDFSLocalConnection.class);

    private LocalFileSystem m_fs = null;
    private final Configuration m_conf;
    private final ConnectionInformation m_connectionInformation;

    /**
     * @param connectionInformation the {@link ConnectionInformation}to use
     */
    public HDFSLocalConnection(final ConnectionInformation connectionInformation) {
        m_connectionInformation = connectionInformation;
        m_conf = new Configuration();
        final String defaultName = createDefaultName(connectionInformation);
        LOGGER.debug("Adding " + CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY + " to config: " + defaultName);
        m_conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultName);
        m_conf.setBoolean(CommonConfigurationKeysPublic.FS_AUTOMATIC_CLOSE_KEY, true);


        final CommonConfigContainer configContainer = CommonConfigContainer.getInstance();
        if (configContainer.hasCoreSiteConfig()) {
            m_conf.addResource(configContainer.getCoreSiteConfig());
        }
        if (configContainer.hasHdfsSiteConfig()) {
            m_conf.addResource(configContainer.getHdfsSiteConfig());
        }
    }

    private static String createDefaultName(final ConnectionInformation conInfo) {
        final String defaultName = conInfo.getProtocol()
                + "://" + conInfo.getHost() + ":" + conInfo.getPort() + "/";
        return defaultName;
    }

    /**
     * @param uri the {@link URI} to convert to a hdfs {@link Path}
     * @return the hdfs {@link Path} for the given {@link URI}
     */
    public Path getHDFSPath4URI(final URI uri) {
        return getHDFSPath4URI(m_connectionInformation, uri);
    }

    /**
     * @param connectionInformation the {@link ConnectionInformation}
     * @param uri the {@link URI} to convert
     * @return the {@link Path}
     */
    public static Path getHDFSPath4URI(final ConnectionInformation connectionInformation, final URI uri) {
        final StringBuilder hdfsPath = new StringBuilder();
        if (uri.getScheme() != null) {
            hdfsPath.append(HDFSLocalRemoteFileHandler.mapScheme(uri.getScheme()));
        } else {
            hdfsPath.append(HDFSLocalRemoteFileHandler.mapScheme(connectionInformation.getProtocol()));
        }
        hdfsPath.append("://");
        if (uri.getPath() == null || uri.getPath().trim().isEmpty()) {
            hdfsPath.append("/");
        } else {
            hdfsPath.append(uri.getPath());
        }
        return new Path(hdfsPath.toString());
    }

    /**
     * @return the {@link FileSystem}
     * @throws IOException if the {@link FileSystem} cannot accessed
     */
    public synchronized LocalFileSystem getFileSystem() throws IOException {
        if (!isOpen()) {
            open();
        }
        return m_fs;
    }

    /**
     * The source file is on the local disk.  Add it to Hadoop file system with this connection at the given
     * destination name.
     * @param delSrc whether to delete the src
     * @param overwrite whether to overwrite an existing file
     * @param src path
     * @param dst path
     * @throws IOException if an exception occurs
     */
    public synchronized void copyFromLocalFile(final boolean delSrc, final boolean overwrite, final URI src,
        final URI dst) throws IOException {
        final LocalFileSystem fs = getFileSystem();
        fs.copyFromLocalFile(delSrc, overwrite, new Path(src), getHDFSPath4URI(dst));
    }

    /**
     * The src file is under FS, and the dst is on the local disk. Copy it from FS control to the local dst name.
     * delSrc indicates if the src will be removed or not. useRawLocalFileSystem indicates whether to use
     * RawLocalFileSystem as local file system or not. RawLocalFileSystem is non src file system.
     * So, It will not create any src files at local.
     *
     * @param delSrc whether to delete the src
     * @param src path
     * @param dst path
     * @param useRawLocalFileSystem whether to use RawLocalFileSystem as local file system or not.
     *
     * @throws IOException - if any IO error
     */
    public synchronized void copyToLocalFile(final boolean delSrc, final URI src, final URI dst,
        final boolean useRawLocalFileSystem) throws IOException {
        final LocalFileSystem fs = getFileSystem();
        fs.copyToLocalFile(delSrc, getHDFSPath4URI(src), new Path(dst), useRawLocalFileSystem);
    }

    /** Delete a file.
    *
    * @param f the path to delete.
    * @param recursive if path is a directory and set to
    * true, the directory is deleted else throws an exception. In
    * case of a file the recursive can be set to either true or false.
    * @return  true if delete is successful else false.
    * @throws IOException
    */
    public synchronized boolean delete(final URI f, final boolean recursive) throws IOException {
        final LocalFileSystem fs = getFileSystem();
        return fs.delete(getHDFSPath4URI(f), recursive);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void open() throws IOException {
        if (m_fs == null) {
            m_fs = FileSystem.getLocal(m_conf);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOpen() {
        return m_fs != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void close() throws IOException {
        if (m_fs != null) {
                    m_fs.close();
        }
    }

    /**
     * @param dir the directory to create
     * @return <code>true</code> if the directory was successfully created
     * @throws IOException if an exception occurs
     */
    public synchronized boolean mkDir(final URI dir) throws IOException {
        final LocalFileSystem fs = getFileSystem();
        return fs.mkdirs(getHDFSPath4URI(dir));
    }

    /**
     * @param uri the {@link URI} to open
     * @return {@link FSDataInputStream} to read from
     * @throws IOException if an exception occurs
     */
    public synchronized FSDataInputStream open(final URI uri) throws IOException {
        final LocalFileSystem fs = getFileSystem();
        return fs.open(getHDFSPath4URI(uri));
    }

    /**
     * Create an FSDataOutputStream at the indicated Path.
     * @param f the file to create
     * @param overwrite if a file with this name already exists, then if true,
     *   the file will be overwritten, and if false an exception will be thrown.
     * @return the {@link FSDataOutputStream} to write to
     * @throws IOException if an exception occurs
     */
    public synchronized FSDataOutputStream create(final URI f, final boolean overwrite)
        throws IOException {
        final FileSystem fs = getFileSystem();
        return fs.create(getHDFSPath4URI(f), overwrite);
    }

    /**
     * @param uri
     * @return <code>true</code> if the path exists
     * @throws IOException if an exception occurs
     */
    public synchronized boolean exists(final URI uri) throws IOException {
        final LocalFileSystem fs = getFileSystem();
        return fs.exists(getHDFSPath4URI(uri));
    }

    /**
     * @param uri the {@link URI} to test
     * @return <code>true</code> if the {@link URI} belongs to a directory
     * @throws IOException if an exception occurs
     */
    public synchronized boolean isDirectory(final URI uri) throws IOException {
        final LocalFileSystem fs = getFileSystem();
        return fs.isDirectory(getHDFSPath4URI(uri));
    }

    /**
     * @param uri the {@link URI} to get the {@link FileStatus} for
     * @return the {@link FileStatus} for the given {@link URI}
     * @throws IOException if an exception occurs
     */
    public synchronized FileStatus getFileStatus(final URI uri) throws IOException {
        final LocalFileSystem fs = getFileSystem();
        final FileStatus fileStatus = fs.getFileStatus(getHDFSPath4URI(uri));
        return fileStatus;
    }

    /**
     * List the statuses of the files/directories in the given path if the path is
     * a directory.
     * @param uri the {@link URI} of the directory to get all contained files and directories for
     * @return array of {@link FileStatus} objects that represent all files and directories within the given directory
     * @throws IOException if an exception occurred
     * @throws FileNotFoundException
     */
    public synchronized FileStatus[] listFiles(final URI uri) throws FileNotFoundException, IOException {
        final LocalFileSystem fs = getFileSystem();
        return fs.listStatus(getHDFSPath4URI(uri));
    }

    /**
     * @param uri the {@link URI} to get the ContentSummary for
     * @return the {@link ContentSummary} of the given URI
     * @throws IOException if an exception occurs
     */
    public synchronized ContentSummary getContentSummary(final URI uri) throws IOException {
        final LocalFileSystem fs = getFileSystem();
        //returns the size of the file or the length of all files within the directory
        final ContentSummary contentSummary = fs.getContentSummary(getHDFSPath4URI(uri));
        return contentSummary;
    }

    /**
     * @param uri the {@link URI} of the file to change the permission for
     * @param unixSymbolicPermission  a Unix symbolic permission string e.g. "-rw-rw-rw-"
     * @throws IOException if an exception occurs
     */
    public synchronized void setPermission(final URI uri, final String unixSymbolicPermission) throws IOException {
        final LocalFileSystem fs = getFileSystem();
        fs.setPermission(getHDFSPath4URI(uri), FsPermission.valueOf(unixSymbolicPermission));
    }
}
