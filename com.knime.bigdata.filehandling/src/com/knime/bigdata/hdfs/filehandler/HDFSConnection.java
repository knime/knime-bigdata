package com.knime.bigdata.hdfs.filehandler;

/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.UserGroupInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.MutableInteger;

import com.knime.bigdata.commons.config.CommonConfigContainer;
import com.knime.bigdata.commons.security.kerberos.UserGroupUtil;
import com.knime.licenses.LicenseException;

/**
 *
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 */
public class HDFSConnection extends Connection {
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
     */
    public HDFSConnection(final ConnectionInformation connectionInformation) {
        try {
            HDFSRemoteFileHandler.LICENSE_CHECKER.checkLicense();
        } catch (LicenseException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        m_connectionInformation = connectionInformation;
        m_conf = new Configuration();
        m_conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, createDefaultName(connectionInformation));
        m_conf.setBoolean(CommonConfigurationKeysPublic.FS_AUTOMATIC_CLOSE_KEY, true);
        if (m_connectionInformation.useKerberos()) {
            //use Kerberos based authentication
            m_conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, AuthMethod.KERBEROS.name());
            //http://stackoverflow.com/questions/37050626/java-failed-to-specify-servers-kerberos-principal-name
            //For Hadoop version less than 2.6.2 the default pattern property is not available in hdfs-site.xml file
            //so we need to specify pattern property manually
            m_conf.set("dfs.namenode.kerberos.principal.pattern", "*");
//        System.setProperty("java.security.krb5.realm", "EXAMPLE.COM");
//        System.setProperty("java.security.krb5.kdc", "sandbox.example.com");
        } else {
            //or simple which is the default
            m_conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, AuthMethod.SIMPLE.name());
        }

        CommonConfigContainer configContainer = CommonConfigContainer.getInstance();
        if (configContainer.hasCoreSiteConfig()) {
            m_conf.addResource(configContainer.getCoreSiteConfig());
        }
        if (configContainer.hasHdfsSiteConfig()) {
            m_conf.addResource(configContainer.getHdfsSiteConfig());
        }
    }

    private static String createDefaultName(final ConnectionInformation conInfo) {
        final String defaultName = conInfo.getProtocol() + "://" + conInfo.getHost() + ":" + conInfo.getPort()
                + getHadoopDefaultPath(conInfo.getUser());
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
            hdfsPath.append(uri.getScheme());
        } else {
            hdfsPath.append(connectionInformation.getProtocol());
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
        String userInfo = uri.getUserInfo();
        if (userInfo == null) {
            userInfo = connectionInformation.getUser();
        }
        hdfsPath.append(getAbsoluteHadoopPath(uri.getPath(), userInfo));
        return new Path(hdfsPath.toString());
    }

    /**
     * @param path the path to convert into an absolute hadoop path
     * @param user the name of the user to access the file
     * @return the absolute hdfs path
     */
    public static String getAbsoluteHadoopPath(final String path, final String user) {
        final String hadoopDefaultPath = getHadoopDefaultPath(user);
        if (path == null || path.trim().isEmpty()) {
            //add the root path to the uri if it does not exist
            return hadoopDefaultPath;
        } else if (!path.startsWith("/")) {
//            if (path.startsWith("/")) {
//                return hadoopDefaultPath + path;
//            }
            return hadoopDefaultPath + "/" + path;
        }
        return path;
    }

    /**
     * @param user the hadoop user
     * @return the default hadoop path for the given user
     */
    public static String getHadoopDefaultPath(final String user) {
        final String getHadoopDefaultPath = "/user/" + user;
        return getHadoopDefaultPath;
    }

    /**
     * @return the {@link FileSystem}
     * @throws IOException if the {@link FileSystem} cannot accessed
     */
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
     */
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
     */
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
    */
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
            synchronized (CONNECTION_COUNT) {
                final String userName;
                if (m_connectionInformation.useKerberos()) {
                    //ensure that no user name is used if Kerberos is enabled since the panel also reports the
                    //user name if the field is disabled
                    userName = null;
                } else {
                    userName = m_connectionInformation.getUser();
                }
                try {
                    final UserGroupInformation user = UserGroupUtil.getUser(m_conf, userName);
                    if (m_connectionInformation.useKerberos()) {
                        //use the short Kerberos user name without the real information as user for the
                        //connection information which is used to create the hdfs path
                        m_connectionInformation.setUser(user.getShortUserName());
                    }
                    m_fs = user.doAs(new PrivilegedExceptionAction<FileSystem>() {
                        @Override
                        public FileSystem run() throws Exception {
                        //the hdfs connections are cached in the FileSystem class based on the uri (which is always
                        //the default one in our case) and the user name which we therefore use as the key for the MAP
                            final FileSystem fs = FileSystem.get(m_conf);
                            final MutableInteger count = CONNECTION_COUNT.get(userName);
                            if (count == null) {
                                CONNECTION_COUNT.put(userName, new MutableInteger(1));
                            } else {
                                count.inc();
                            }
                            return fs;
                        }
                    });
                } catch (Exception e) {
                    LOGGER.debug("Exception while opening HDFS conection: " + e.getMessage(), e);
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
     */
    @SuppressWarnings("resource")
    public synchronized boolean mkDir(final URI dir) throws IOException {
        final FileSystem fs = getFileSystem();
        return fs.mkdirs(getHDFSPath4URI(dir));
    }

    /**
     * @param uri the {@link URI} to open
     * @return {@link FSDataInputStream} to read from
     * @throws IOException if an exception occurs
     */
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
     */
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
     */
    @SuppressWarnings("resource")
    public synchronized boolean exists(final URI uri) throws IOException {
        final FileSystem fs = getFileSystem();
        return fs.exists(getHDFSPath4URI(uri));
    }

    /**
     * @param uri the {@link URI} to test
     * @return <code>true</code> if the {@link URI} belongs to a directory
     * @throws IOException if an exception occurs
     */
    @SuppressWarnings("resource")
    public synchronized boolean isDirectory(final URI uri) throws IOException {
        final FileSystem fs = getFileSystem();
        return fs.isDirectory(getHDFSPath4URI(uri));
    }

    /**
     * @param uri the {@link URI} to get the {@link FileStatus} for
     * @return the {@link FileStatus} for the given {@link URI}
     * @throws IOException if an exception occurs
     */
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
     */
    @SuppressWarnings("resource")
    public synchronized FileStatus[] listFiles(final URI uri) throws FileNotFoundException, IOException {
        final FileSystem fs = getFileSystem();
        return fs.listStatus(getHDFSPath4URI(uri));
    }

    /**
     * @param uri the {@link URI} to get the ContentSummary for
     * @return the {@link ContentSummary} of the given URI
     * @throws IOException if an exception occurs
     */
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
    @SuppressWarnings("resource")
    public synchronized void setPermission(final URI uri, final String unixSymbolicPermission) throws IOException {
        final FileSystem fs = getFileSystem();
        fs.setPermission(getHDFSPath4URI(uri), FsPermission.valueOf(unixSymbolicPermission));
    }
}
