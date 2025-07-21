package org.knime.bigdata.hdfs.filehandler;

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
 *   Created on 31.07.2014 by koetter
 */


import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.bigdata.commons.config.CommonConfigContainer;
import org.knime.bigdata.commons.hadoop.ConfigurationFactory;
import org.knime.bigdata.commons.hadoop.UserGroupUtil;
import org.knime.bigdata.commons.hadoop.UserGroupUtil.UserGroupInformationCallback;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.MutableInteger;

/**
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
@Deprecated
public class HDFSConnection extends Connection implements HDFSCompatibleConnection {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(HDFSConnection.class);
    private FileSystem m_fs = null;
    private final Configuration m_conf;
    private final ConnectionInformation m_connectionInformation;
    //We have to count the open connections per user since connections are cached within the FileSystem class.
    //If two different nodes work with the same hdfs user they work with the same FileSystem instance which
    //they get from the FileSystem.CACHE. Counting the number of open connections helps to prevent that one node
    //closes the connection prior the other has finished!!!
    private static final Map<String, MutableInteger> CONNECTION_COUNT = new HashMap<>();

    /**
     * @param connectionInformation the {@link ConnectionInformation}to use
     * @deprecated
     */
    @Deprecated
    public HDFSConnection(final ConnectionInformation connectionInformation) {
        m_connectionInformation = connectionInformation;

        if (connectionInformation == null) {
            throw new RuntimeException("HDFS connection informations input required.");
        }

        if (m_connectionInformation.useKerberos()) {
            LOGGER.debug("Adding Kerberos settings to configuration");
            m_conf = ConfigurationFactory.createBaseConfigurationWithKerberosAuth();
        } else {
            // or simple which is the default
            m_conf = ConfigurationFactory.createBaseConfigurationWithSimpleAuth();
        }

        // set default
        final String defaultName = createDefaultName(connectionInformation);
        LOGGER.debug("Adding " + CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY + " to config: " + defaultName);
        m_conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultName);

        // Add SSL configuration, wrapper and current class loader
        if (HDFSRemoteFileHandler.isEncryptedConnection(connectionInformation)) {
            if (CommonConfigContainer.getInstance().hasSSLConfig()) {
                CommonConfigContainer.getInstance().addSSLConfig(m_conf);
                m_conf.set(SSLFactory.KEYSTORES_FACTORY_CLASS_KEY, KeyStoreFactoryWrapper.class.getCanonicalName());
                m_conf.setClassLoader(getClass().getClassLoader());
            } else {
                final String msg = "HDFS SSL settings required by connection settings, but not set in preferences (See KNIME > Big Data > Hadoop)!";
                LOGGER.warn(msg);
                throw new RuntimeException(msg);
            }
        }
    }

    private static String createDefaultName(final ConnectionInformation conInfo) {
        final String defaultName = HDFSRemoteFileHandler.mapScheme(conInfo.getProtocol())
                + "://" + conInfo.getHost() + ":" + conInfo.getPort() + "/";
        return defaultName;
    }

    /**
     * @param uri the {@link URI} to convert to a hdfs {@link Path}
     * @return the hdfs {@link Path} for the given {@link URI}
     * @deprecated
     */
    @Deprecated
    public Path getHDFSPath4URI(final URI uri) {
        return getHDFSPath4URI(m_connectionInformation, uri);
    }

    /**
     * @param connectionInformation the {@link ConnectionInformation}
     * @param uri the {@link URI} to convert
     * @return the {@link Path}
     * @deprecated
     */
    @Deprecated
    public static Path getHDFSPath4URI(final ConnectionInformation connectionInformation, final URI uri) {
        final StringBuilder hdfsPath = new StringBuilder();
        if (uri.getScheme() != null) {
            hdfsPath.append(HDFSRemoteFileHandler.mapScheme(uri.getScheme()));
        } else {
            hdfsPath.append(HDFSRemoteFileHandler.mapScheme(connectionInformation.getProtocol()));
        }
        hdfsPath.append("://");
        if (uri.getHost() != null) {
            hdfsPath.append(uri.getHost());
        } else {
            hdfsPath.append(connectionInformation.getHost());
        }
        hdfsPath.append(":");
        if (uri.getPort() >= 0) {
            hdfsPath.append(uri.getPort());
        } else {
            hdfsPath.append(connectionInformation.getPort());
        }
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
     * @deprecated
     */
    @Deprecated
    public synchronized FileSystem getFileSystem() throws IOException {
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
     * @deprecated
     */
    @Deprecated
    public synchronized void copyFromLocalFile(final boolean delSrc, final boolean overwrite, final URI src,
        final URI dst) throws IOException {
        @SuppressWarnings("resource")
        final FileSystem fs = getFileSystem();
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
     * @deprecated
     */
    @Deprecated
    public synchronized void copyToLocalFile(final boolean delSrc, final URI src, final URI dst,
        final boolean useRawLocalFileSystem) throws IOException {
        @SuppressWarnings("resource")
        final FileSystem fs = getFileSystem();
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
    * @deprecated
    */
    @Deprecated
    @SuppressWarnings("resource")
    public synchronized boolean delete(final URI f, final boolean recursive) throws IOException {
        final FileSystem fs = getFileSystem();
        return fs.delete(getHDFSPath4URI(f), recursive);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void open() throws IOException {
        if (m_fs == null) {
            final UserGroupInformationCallback<FileSystem> fsCallback = (ugi) -> {
                LOGGER.debug(String.format("Creating %s connection with user: %s", m_connectionInformation.getProtocol(),
                    ugi.toString()));
                final String userName =
                    m_connectionInformation.useKerberos() ? ugi.getShortUserName() : m_connectionInformation.getUser();

                m_connectionInformation.setUser(userName);

                //the hdfs connections are cached in the FileSystem class based on the uri (which is always
                //the default one in our case) and the user name which we therefore use as the key for the MAP
                final FileSystem fs = ugi.doAs((PrivilegedExceptionAction<FileSystem>)() -> FileSystem.get(m_conf));
                final MutableInteger count = CONNECTION_COUNT.get(userName);
                if (count == null) {
                    CONNECTION_COUNT.put(userName, new MutableInteger(1));
                } else {
                    count.inc();
                }
                return fs;
            };

            synchronized (CONNECTION_COUNT) {
                try {
                    if (m_connectionInformation.useKerberos()) {
                        m_fs = UserGroupUtil.runWithProxyUserUGIIfNecessary(fsCallback);
                    } else {
                        m_fs = UserGroupUtil.runWithRemoteUserUGI(m_connectionInformation.getUser(), fsCallback);
                    }
                } catch (Exception e) {
                    throw new IOException(e.getMessage(), e.getCause());
                }
            }
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
            synchronized (CONNECTION_COUNT) {
                final String userName = m_connectionInformation.getUser();
                final MutableInteger counter = CONNECTION_COUNT.get(userName);
                if (counter == null || counter.intValue() <= 1) {
                    //this is the last one that held the connection open => close it
                    m_fs.close();
                    CONNECTION_COUNT.remove(userName);
                } else {
                    counter.dec();
                }
                m_fs = null;
            }
        }
    }

    /**
     * @param dir the directory to create
     * @return <code>true</code> if the directory was successfully created
     * @throws IOException if an exception occurs
     * @deprecated
     */
    @Deprecated
    @SuppressWarnings("resource")
    public synchronized boolean mkDir(final URI dir) throws IOException {
        final FileSystem fs = getFileSystem();
        return fs.mkdirs(getHDFSPath4URI(dir));
    }

    /**
     * @param uri the {@link URI} to open
     * @return {@link FSDataInputStream} to read from
     * @throws IOException if an exception occurs
     * @deprecated
     */
    @Deprecated
    @SuppressWarnings("resource")
    public synchronized FSDataInputStream open(final URI uri) throws IOException {
        final FileSystem fs = getFileSystem();
        return fs.open(getHDFSPath4URI(uri));
    }

    /**
     * Create an FSDataOutputStream at the indicated Path.
     * @param f the file to create
     * @param overwrite if a file with this name already exists, then if true,
     *   the file will be overwritten, and if false an exception will be thrown.
     * @return the {@link FSDataOutputStream} to write to
     * @throws IOException if an exception occurs
     * @deprecated
     */
    @Deprecated
    @SuppressWarnings("resource")
    public synchronized FSDataOutputStream create(final URI f, final boolean overwrite)
        throws IOException {
        final FileSystem fs = getFileSystem();
        return fs.create(getHDFSPath4URI(f), overwrite);
    }

    /**
     * @param uri
     * @return <code>true</code> if the path exists
     * @throws IOException if an exception occurs
     * @deprecated
     */
    @Deprecated
    @Override
    @SuppressWarnings("resource")
    public synchronized boolean exists(final URI uri) throws IOException {
        final FileSystem fs = getFileSystem();
        return fs.exists(getHDFSPath4URI(uri));
    }

    /**
     * @param uri the {@link URI} to test
     * @return <code>true</code> if the {@link URI} belongs to a directory
     * @throws IOException if an exception occurs
     * @deprecated
     */
    @Deprecated
    @SuppressWarnings("resource")
    public synchronized boolean isDirectory(final URI uri) throws IOException {
        final FileSystem fs = getFileSystem();
        return fs.isDirectory(getHDFSPath4URI(uri));
    }

    /**
     * @param uri the {@link URI} to get the {@link FileStatus} for
     * @return the {@link FileStatus} for the given {@link URI}
     * @throws IOException if an exception occurs
     * @deprecated
     */
    @Deprecated
    @SuppressWarnings("resource")
    public synchronized FileStatus getFileStatus(final URI uri) throws IOException {
        final FileSystem fs = getFileSystem();
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
     * @deprecated
     */
    @Deprecated
    @SuppressWarnings("resource")
    public synchronized FileStatus[] listFiles(final URI uri) throws FileNotFoundException, IOException {
        final FileSystem fs = getFileSystem();
        return fs.listStatus(getHDFSPath4URI(uri));
    }

    /**
     * @param uri the {@link URI} to get the ContentSummary for
     * @return the {@link ContentSummary} of the given URI
     * @throws IOException if an exception occurs
     * @deprecated
     */
    @Deprecated
    @SuppressWarnings("resource")
    public synchronized ContentSummary getContentSummary(final URI uri) throws IOException {
        final FileSystem fs = getFileSystem();
        //returns the size of the file or the length of all files within the directory
        final ContentSummary contentSummary = fs.getContentSummary(getHDFSPath4URI(uri));
        return contentSummary;
    }

    /**
     * @param uri the {@link URI} of the file to change the permission for
     * @param unixSymbolicPermission  a Unix symbolic permission string e.g. "-rw-rw-rw-"
     * @throws IOException if an exception occurs
     */
    @Override
    @SuppressWarnings("resource")
    public synchronized void setPermission(final URI uri, final String unixSymbolicPermission) throws IOException {
        final FileSystem fs = getFileSystem();
        fs.setPermission(getHDFSPath4URI(uri), FsPermission.valueOf(unixSymbolicPermission));
    }

    @Override
    public synchronized URI getHomeDirectory() throws IOException {
        return getFileSystem().getHomeDirectory().toUri();
    }
}
