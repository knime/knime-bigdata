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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.knime.filehandling.core.connections.base.BaseFileSystemProvider;
import org.knime.filehandling.core.connections.base.attributes.BaseFileAttributes;
import org.knime.filehandling.core.connections.base.attributes.BasePrincipal;

/**
 * {@link HdfsFileSystem} file system provider.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class HdfsFileSystemProvider extends BaseFileSystemProvider<HdfsPath, HdfsFileSystem> {

    @SuppressWarnings("resource")
    private FileSystem getHadoopFS() {
        return getFileSystemInternal().getHadoopFileSystem();
    }

    @Override
    protected SeekableByteChannel newByteChannelInternal(final HdfsPath path, final Set<? extends OpenOption> options,
        final FileAttribute<?>... attrs) throws IOException {

        return new HdfsSeekableByteChannel(path, options);
    }

    @SuppressWarnings("resource")
    @Override
    protected void moveInternal(final HdfsPath source, final HdfsPath target, final CopyOption... options) throws IOException {
        final FileSystem fs = getHadoopFS();
        final Path sourcePath = source.toHadoopPath();
        final Path targetPath = target.toHadoopPath();

        try {
            // remove target if required
            if (existsCached(target) && ArrayUtils.contains(options, StandardCopyOption.REPLACE_EXISTING)) {
                delete(target);
            }

            if (!fs.rename(sourcePath, targetPath)) {
                throw new IOException("Failed to move '" + source + "' to '" + target + "' (hadoop returns false).");
            }
        } catch (final Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e, source, target);
        }
    }

    /**
     * {@inheritDoc}
     *
     * Note: HDFS does not support copy and therefore the file will be downloaded and uploaded again.
     */
    @SuppressWarnings("resource")
    @Override
    protected void copyInternal(final HdfsPath source, final HdfsPath target, final CopyOption... options) throws IOException {
        final FileSystem fs = getHadoopFS();
        final boolean deleteSource = false;
        final boolean overwrite = ArrayUtils.contains(options, StandardCopyOption.REPLACE_EXISTING);
        final Path sourcePath = source.toHadoopPath();
        final Path targetPath = target.toHadoopPath();

        // remove target if required
        if (existsCached(target) && ArrayUtils.contains(options, StandardCopyOption.REPLACE_EXISTING)) {
            delete(target);
        }

        try {
            if (Files.isDirectory(source)) {
                createDirectoryInternal(target);
            } else if (!FileUtil.copy(fs, sourcePath, fs, targetPath, deleteSource, overwrite, fs.getConf())) {
                throw new IOException("Failed to copy '" + source + "' to '" + target + "' (hadoop returns false).");
            } else {
                // copy was successful
            }
        } catch (final Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e, source, target);
        }
    }

    /**
     * Validate if the given path is an empty directory using the content summary instead of listing all possible files.
     *
     * @return {@code true} if the given path contains at least one child file or directory, {@code false} otherwise
     */
    @SuppressWarnings("resource")
    private boolean isNonEmptyDir(final HdfsPath path) throws IOException {
        final FileSystem fs = getHadoopFS();
        final Path hadoopPath = path.toHadoopPath();

        // The summary includes the target in the file count, if it is a file
        // or the directory in the directory count if it is a directory.
        final ContentSummary summary = fs.getContentSummary(hadoopPath);
        return summary.getFileCount() + summary.getDirectoryCount() > 1;
    }

    @SuppressWarnings("resource")
    @Override
    protected InputStream newInputStreamInternal(final HdfsPath path, final OpenOption... options) throws IOException {
        try {
            return getHadoopFS().open(path.toHadoopPath());
        } catch (final Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e, path);
        }
    }

    @SuppressWarnings("resource")
    @Override
    protected OutputStream newOutputStreamInternal(final HdfsPath path, final OpenOption... options) throws IOException {
        try {
            if (ArrayUtils.contains(options, StandardOpenOption.APPEND)) {
                return getHadoopFS().append(path.toHadoopPath());
            } else {
                final boolean overwrite = ArrayUtils.contains(options, StandardOpenOption.TRUNCATE_EXISTING);
                return getHadoopFS().create(path.toHadoopPath(), overwrite);
            }
        } catch (final Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e, path);
        }
    }

    @SuppressWarnings("resource")
    @Override
    protected Iterator<HdfsPath> createPathIterator(final HdfsPath path, final Filter<? super java.nio.file.Path> filter) throws IOException {
        try {
            final Path hadoopPath = path.toHadoopPath();
            final FileStatus[] stats = getHadoopFS().listStatus(hadoopPath);
            return new HdfsPathIterator(path, hadoopPath, stats, filter);
        } catch (final Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e, path);
        }
    }

    @SuppressWarnings("resource")
    @Override
    protected void createDirectoryInternal(final HdfsPath path, final FileAttribute<?>... attrs) throws IOException {
        try {
            if (!getHadoopFS().mkdirs(path.toHadoopPath())) {
                throw new IOException("Failed to create directory '" + path + "' (hadoop returns false).");
            }
        } catch (final Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e, path);
        }
    }

    @SuppressWarnings("resource")
    @Override
    protected boolean exists(final HdfsPath path) throws IOException {
        try {
            return getHadoopFS().exists(path.toHadoopPath());
        } catch (final AccessControlException e) { // NOSONAR
            final java.nio.file.Path parent = path.getParent();

            // throw a file system exception if the parent is not a directory
            if (parent != null && e.getMessage() != null
                    && e.getMessage().startsWith(parent + " (is not a directory)")) {
                return false;
            } else {
                throw ExceptionMapper.mapException(e, path);
            }
        } catch (final Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e, path);
        }
    }

    @SuppressWarnings("resource")
    @Override
    protected BaseFileAttributes fetchAttributesInternal(final HdfsPath path, final Class<?> type) throws IOException {
        try {
            final FileStatus status = getHadoopFS().getFileStatus(path.toHadoopPath());
            return toBaseFileAttributes(path, status);
        } catch (final Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e, path);
        }
    }

    /**
     * Convert given {@link FileStatus} into #{@link BaseFileAttributes}.
     *
     * @param path path
     * @param status attributes to convert
     * @return converted attributes
     */
    protected static BaseFileAttributes toBaseFileAttributes(final HdfsPath path, final FileStatus status) {
        return new BaseFileAttributes(
            status.isFile(), //
            path, //
            FileTime.fromMillis(status.getModificationTime()), //
            FileTime.fromMillis(status.getAccessTime()), //
            FileTime.fromMillis(status.getModificationTime()), //
            status.getLen(), //
            status.isSymlink(), //
            false, // is other
            new BasePrincipal(status.getOwner()), //
            new BasePrincipal(status.getGroup()), //
            toPosixFilePermissions(status.getPermission()));
    }

    /**
     * Convert {@link FsPermission} into a set of {@link PosixFilePermission} without sticky bit (not supported by
     * {@link PosixFilePermission}).
     */
    private static Set<PosixFilePermission> toPosixFilePermissions(final FsPermission perm) {
        if (perm.getStickyBit()) {
            final FsPermission stickyBitFree =
                new FsPermission(perm.getUserAction(), perm.getGroupAction(), perm.getOtherAction(), false);
            return PosixFilePermissions.fromString(stickyBitFree.toString());
        } else {
            return PosixFilePermissions.fromString(perm.toString());
        }
    }

    @Override
    protected void checkAccessInternal(final HdfsPath path, final AccessMode... modes) throws IOException {
        // HTTPFS on Hadoop < 3.3.0 does not implement this and therefore we can't implement this here either...
        // See: https://issues.apache.org/jira/browse/HDFS-9695
    }

    @SuppressWarnings("resource")
    @Override
    protected void deleteInternal(final HdfsPath path) throws IOException {
        try {
            final boolean recursive = false;
            getHadoopFS().delete(path.toHadoopPath(), recursive);
        } catch (final Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e, path);
        }
    }

    @Override
    protected boolean isHiddenInternal(final HdfsPath path) throws IOException {
        if (path.isRoot()) {
            return false;
        } else {
            final String name = path.getFileName().toString();
            return name.startsWith(".") || name.startsWith("_");
        }
    }
}
