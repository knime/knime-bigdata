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
 *   Created on Aug 11, 2020 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.delta.util;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;
import org.knime.bigdata.hadoop.filesystem.NioFileSystem;
import org.knime.filehandling.core.connections.FSFileSystem;
import org.knime.filehandling.core.connections.FSPath;

/**
 * Hadoop filesystem that wraps a {@link FSFileSystem}.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class NioAbstractFileSystem extends AbstractFileSystem {

    /**
     * Scheme of this file system.
     */
    public static final String SCHEME = "nio-wrapper";

    /**
     * A {@link ThreadLocal} to transfer the {@link FSFileSystem} into a new instance of this file system.
     */
    static final ThreadLocal<FSFileSystem<?>> NEXT_FILE_SYSTEM = new ThreadLocal<>();

    private URI m_uri;

    private FSFileSystem<?> m_fsFileSystem;

    /**
     * @throws URISyntaxException
     *
     */
    public NioAbstractFileSystem(final URI uri, final Configuration conf) throws URISyntaxException {
        super(uri, NioFileSystem.SCHEME, false, 0);
        m_uri = uri;
        m_fsFileSystem = NEXT_FILE_SYSTEM.get();
        Validate.notNull(m_fsFileSystem, "Unable to get wrapped file system.");
    }

    /**
     * Default constructor.
     *
     * @throws URISyntaxException
     */
    public NioAbstractFileSystem(final URI uri, final String supportedScheme, final boolean authorityNeeded,
        final int defaultPort) throws URISyntaxException {
        super(uri, supportedScheme, authorityNeeded, defaultPort);
        m_uri = uri;
        m_fsFileSystem = NEXT_FILE_SYSTEM.get();
        Validate.notNull(m_fsFileSystem, "Unable to get wrapped file system.");
    }



    @Override
    public URI getUri() {
        return m_uri;
    }

    private FSPath toFSPath(final Path p) {
        String absolutePath = p.toUri().getPath();

        // on windows we have to remove the leading / before the drive
        if (absolutePath.startsWith("/") && Path.isWindowsAbsolutePath(absolutePath, true)) {
            absolutePath = absolutePath.substring(1);
        }

        return m_fsFileSystem.getPath(absolutePath.replace("/", m_fsFileSystem.getSeparator()));
    }

    private Path toHadoopPath(final FSPath p) throws IOException {
        try {
            // the underling file system might use an other separator,
            // convert the path to a compatible URI syntax instead of p.toString here.
            return new Path(new URI(SCHEME, m_uri.getHost(), p.getURICompatiblePath(), null));
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @SuppressWarnings("resource")
    @Override
    public FSDataInputStream open(final Path p, final int bufferSize) throws IOException {
        final Set<OpenOption> options = new HashSet<>();
        options.add(StandardOpenOption.READ);
        return new FSDataInputStream(new BufferedFSInputStream(
            new NioFSInputStream(Files.newByteChannel(toFSPath(p), StandardOpenOption.READ)),
            bufferSize));
    }

    /**
     * {@inheritDoc}
     *
     * The {@link FSDataOutputStream} closes the underlying output stream and resource warnings can be ignored here,
     */
    @SuppressWarnings("resource")
    public FSDataOutputStream create(final Path p, final FsPermission permission, final boolean overwrite,
        final int bufferSize, final short replication, final long blockSize, final Progressable progress)
        throws IOException {
        if (overwrite) {
            return new FSDataOutputStream(new BufferedOutputStream(
                Files.newOutputStream(toFSPath(p), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING),
                bufferSize), statistics);
        } else {
            return new FSDataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(toFSPath(p), StandardOpenOption.CREATE_NEW), bufferSize),
                statistics);
        }
    }

    /**
     * {@inheritDoc}
     *
     * The {@link FSDataOutputStream} closes the underlying output stream and resource warnings can be ignored here,
     */
    @SuppressWarnings("resource")
    public FSDataOutputStream append(final Path p, final int bufferSize, final Progressable progress)
        throws IOException {
        return new FSDataOutputStream(
            new BufferedOutputStream(Files.newOutputStream(toFSPath(p), StandardOpenOption.APPEND), bufferSize),
            statistics);
    }

    public boolean rename(final Path src, final Path dst) throws IOException {
        Files.move(toFSPath(src), toFSPath(dst));
        return true;
    }

    @Override
    public boolean delete(final Path p, final boolean recursive) throws IOException {
        Files.delete(toFSPath(p));
        return true;
    }

    @Override
    public FileStatus[] listStatus(final Path p) throws IOException {
        final FileStatus pathStatus = getFileStatus(p);

        if (pathStatus.isDirectory()) {
            final ArrayList<FileStatus> status = new ArrayList<>();
            try (final DirectoryStream<java.nio.file.Path> stream = Files.newDirectoryStream(toFSPath(p), f -> true)) {
                for (final java.nio.file.Path javaPath : stream) {
                    final FSPath fsPath = (FSPath)javaPath;
                    status.add(getFileStatus(toHadoopPath(fsPath), fsPath));
                }
                return status.toArray(new FileStatus[0]);
            }
        } else {
            return new FileStatus[]{pathStatus};
        }
    }

    public void setWorkingDirectory(final Path p) {
        // not supported
    }

    public Path getWorkingDirectory() {
        try {
            return toHadoopPath(m_fsFileSystem.getWorkingDirectory());
        } catch (final IOException e) {
            throw new UncheckedIOException(new IOException(e));
        }
    }

    @Override
    public FileStatus getFileStatus(final Path p) throws IOException {
        try {
            return getFileStatus(p, toFSPath(p));
        } catch (final NoSuchFileException e) { // NOSONAR
            throw new FileNotFoundException(p.toString());
        }
    }

    private static FileStatus getFileStatus(final Path hadoopPath, final FSPath fsPath) throws IOException {
        final BasicFileAttributes attributes = Files.readAttributes(fsPath, BasicFileAttributes.class);
        return new FileStatus(attributes.size(), attributes.isDirectory(), 0, // block_replication
            0, // blocksize
            attributes.lastModifiedTime().toMillis(), attributes.lastAccessTime().toMillis(), null, // permission
            null, // owner
            null, // group
            hadoopPath);
    }

    @Override
    public int getUriDefaultPort() {
        return 0;
    }

    @Override
    public FsServerDefaults getServerDefaults() throws IOException {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FSDataOutputStream createInternal(final Path f, final EnumSet<CreateFlag> flag,
        final FsPermission absolutePermission, final int bufferSize, final short replication, final long blockSize,
        final Progressable progress, final ChecksumOpt checksumOpt, final boolean createParent)
        throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException,
        UnsupportedFileSystemException, UnresolvedLinkException, IOException {
        // TK_TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void mkdir(final Path dir, final FsPermission permission, final boolean createParent)
        throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, UnresolvedLinkException,
        IOException {
        if (createParent) {
            Files.createDirectories(toFSPath(dir));
        } else {
            Files.createDirectory(toFSPath(dir));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean setReplication(final Path f, final short replication)
        throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        // TK_TODO Auto-generated method stub
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void renameInternal(final Path src, final Path dst)
        throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException,
        UnresolvedLinkException, IOException {
        // TK_TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setPermission(final Path f, final FsPermission permission)
        throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        // TK_TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setOwner(final Path f, final String username, final String groupname)
        throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        // TK_TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setTimes(final Path f, final long mtime, final long atime)
        throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        // TK_TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileChecksum getFileChecksum(final Path f)
        throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        // TK_TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockLocation[] getFileBlockLocations(final Path f, final long start, final long len)
        throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        // TK_TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FsStatus getFsStatus() throws AccessControlException, FileNotFoundException, IOException {
        // TK_TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setVerifyChecksum(final boolean verifyChecksum) throws AccessControlException, IOException {
        // TK_TODO Auto-generated method stub

    }
}
