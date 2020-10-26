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
package org.knime.bigdata.hadoop.filesystem;

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
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.knime.filehandling.core.connections.FSFileSystem;
import org.knime.filehandling.core.connections.FSPath;

/**
 * Hadoop filesystem that wraps a {@link FSFileSystem}.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class NioFileSystem extends FileSystem {

    /**
     * Scheme of this file system.
     */
    public static final String SCHEME = "knime-fs-wrapper";

    /**
     * A {@link ThreadLocal} to transfer the {@link FSFileSystem} into a new instance of this file system.
     */
    static final ThreadLocal<FSFileSystem<?>> NEXT_FILE_SYSTEM = new ThreadLocal<>();

    private URI m_uri;
    private FSFileSystem<?> m_fsFileSystem;

    /**
     * Default constructor.
     */
    public NioFileSystem() {
        // nothing to do, see initialize method
    }

    /**
     * {@inheritDoc}
     *
     * The file system returned by thread local should not be closed here,
     * ignore file system resource warning here.
     */
    @SuppressWarnings("resource")
    @Override
    public void initialize(final URI uri, final Configuration conf) throws IOException {
        super.initialize(uri, conf);
        m_uri = uri;
        m_fsFileSystem = NEXT_FILE_SYSTEM.get();
        Validate.notNull(m_fsFileSystem, "Unable to get wrapped file system.");
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public URI getUri() {
        return m_uri;
    }

    private FSPath toFSPath(final Path p) {
        return m_fsFileSystem.getPath(p.toUri().getPath());
    }

    private Path toHadoopPath(final FSPath p) throws IOException {
        try {
            return new Path(new URI(SCHEME, m_uri.getHost(), p.toString(), null));
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
    @Override
    public FSDataOutputStream create(final Path p, final FsPermission permission, final boolean overwrite, final int bufferSize,
        final short replication, final long blockSize, final Progressable progress) throws IOException {
        if (overwrite) {
            return new FSDataOutputStream(new BufferedOutputStream(
                Files.newOutputStream(toFSPath(p), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING), bufferSize),
                statistics);
        } else {
            return new FSDataOutputStream(
                new BufferedOutputStream(
                    Files.newOutputStream(toFSPath(p), StandardOpenOption.CREATE_NEW), bufferSize),
                statistics);
        }
    }

    /**
     * {@inheritDoc}
     *
     * The {@link FSDataOutputStream} closes the underlying output stream and resource warnings can be ignored here,
     */
    @SuppressWarnings("resource")
    @Override
    public FSDataOutputStream append(final Path p, final int bufferSize, final Progressable progress) throws IOException {
        return new FSDataOutputStream(new BufferedOutputStream(
            Files.newOutputStream(toFSPath(p), StandardOpenOption.APPEND), bufferSize), statistics);
    }

    @Override
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
        final ArrayList<FileStatus> status = new ArrayList<>();
        try (final DirectoryStream<java.nio.file.Path> stream =
            Files.newDirectoryStream(toFSPath(p), f -> true)) {
            for (final java.nio.file.Path javaPath : stream) {
                final FSPath fsPath = (FSPath) javaPath;
                status.add(getFileStatus(toHadoopPath(fsPath), fsPath));
            }
            return status.toArray(new FileStatus[0]);
        }
    }

    @Override
    public void setWorkingDirectory(final Path p) {
        // not supported
    }

    @Override
    public Path getWorkingDirectory() {
        try {
            final String path = m_fsFileSystem.getWorkingDirectory().toString();
            return new Path(new URI(SCHEME, m_uri.getHost(), path, null));
        } catch (final URISyntaxException e) {
            throw new UncheckedIOException(new IOException(e));
        }
    }

    @Override
    public boolean mkdirs(final Path p, final FsPermission permission) throws IOException {
        Files.createDirectory(toFSPath(p));
        return true;
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
        return new FileStatus(attributes.size(),
            attributes.isDirectory(),
            0, // block_replication
            0, // blocksize
            attributes.lastModifiedTime().toMillis(),
            attributes.lastAccessTime().toMillis(),
            null, // permission
            null, // owner
            null, // group
            hadoopPath);
    }
}
