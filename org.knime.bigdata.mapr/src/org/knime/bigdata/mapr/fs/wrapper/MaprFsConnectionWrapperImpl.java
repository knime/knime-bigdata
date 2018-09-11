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
 *   Created on Sep 9, 2018 by bjoern
 */
package org.knime.bigdata.mapr.fs.wrapper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
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
import org.knime.core.util.MutableInteger;

/**
 *
 * @author bjoern
 */
public class MaprFsConnectionWrapperImpl implements MaprFsConnectionWrapper {

    private FileSystem m_fs = null;

    private final Configuration m_conf;

    //We have to count the open connections per user since connections are cached within the FileSystem class.
    //If two different nodes work with the same hdfs user they work with the same FileSystem instance which
    //they get from the FileSystem.CACHE. Counting the number of open connections helps to prevent that one node
    //closes the connection prior the other has finished!!!
    private static final Map<String, MutableInteger> CONNECTION_COUNT = new HashMap<>();

    public MaprFsConnectionWrapperImpl(final boolean useKerberos) {
        // we need to replace the current context class loader (which comes from OSGI)
        // with the spark class loader, otherwise Java's ServiceLoader frame does not
        // work properly which breaks Spark's DataSource API
        final ClassLoader origContextClassLoader = swapContextClassLoader();

        try {
            m_conf = new Configuration();
            if (useKerberos) {
                m_conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, AuthMethod.KERBEROS.name());
            } else {
                m_conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, AuthMethod.SIMPLE.name());
            }
        } finally {
            Thread.currentThread().setContextClassLoader(origContextClassLoader);
        }
    }

    private ClassLoader swapContextClassLoader() {
        final ClassLoader contextClassLoaderBackup = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
        return contextClassLoaderBackup;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void open() throws IOException {
        if (m_fs == null) {
            // we need to replace the current context class loader (which comes from OSGI)
            // with the spark class loader, otherwise Java's ServiceLoader frame does not
            // work properly which breaks Spark's DataSource API
            final ClassLoader origContextClassLoader = swapContextClassLoader();

            try {
                //the hdfs connections are cached in the FileSystem class based on the uri (which is always
                //the default one in our case) and the user name which we therefore use as the key for the MAP
                m_fs = FileSystem.get(m_conf);
            } finally {
                Thread.currentThread().setContextClassLoader(origContextClassLoader);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized boolean isOpen() {
        return m_fs != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void close() throws IOException {
        if (m_fs != null) {
            m_fs.close();
            m_fs = null;
        }
    }

    /**
     * @return the {@link FileSystem}
     * @throws Exception if the {@link FileSystem} cannot accessed
     */
    public synchronized FileSystem getFileSystem() throws IOException {
        if (!isOpen()) {
            open();
        }
        return m_fs;
    }

    /**
     * The source file is on the local disk. Add it to Hadoop file system with this connection at the given destination
     * name.
     *
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

    private static Path getHDFSPath4URI(final URI dst) {
        return new Path(dst.getPath());
    }

    /**
     * The src file is under FS, and the dst is on the local disk. Copy it from FS control to the local dst name. delSrc
     * indicates if the src will be removed or not. useRawLocalFileSystem indicates whether to use RawLocalFileSystem as
     * local file system or not. RawLocalFileSystem is non src file system. So, It will not create any src files at
     * local.
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

    /**
     * Delete a file.
     *
     * @param f the path to delete.
     * @param recursive if path is a directory and set to true, the directory is deleted else throws an exception. In
     *            case of a file the recursive can be set to either true or false.
     * @return true if delete is successful else false.
     * @throws IOException
     */
    @SuppressWarnings("resource")
    public synchronized boolean delete(final URI f, final boolean recursive) throws IOException {
        final FileSystem fs = getFileSystem();
        return fs.delete(getHDFSPath4URI(f), recursive);
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
     *
     * @param f the file to create
     * @param overwrite if a file with this name already exists, then if true, the file will be overwritten, and if
     *            false an exception will be thrown.
     * @return the {@link FSDataOutputStream} to write to
     * @throws IOException if an exception occurs
     */
    @SuppressWarnings("resource")
    public synchronized FSDataOutputStream create(final URI f, final boolean overwrite) throws IOException {
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
     * List the statuses of the files/directories in the given path if the path is a directory.
     *
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
     * @param unixSymbolicPermission a Unix symbolic permission string e.g. "-rw-rw-rw-"
     * @throws IOException if an exception occurs
     */
    @SuppressWarnings("resource")
    public synchronized void setPermission(final URI uri, final String unixSymbolicPermission) throws IOException {
        final FileSystem fs = getFileSystem();
        fs.setPermission(getHDFSPath4URI(uri), FsPermission.valueOf(unixSymbolicPermission));
    }
}
