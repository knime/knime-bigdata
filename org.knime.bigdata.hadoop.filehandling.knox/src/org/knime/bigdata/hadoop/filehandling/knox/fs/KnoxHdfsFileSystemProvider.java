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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.web.resources.DeleteOpParam;
import org.apache.hadoop.hdfs.web.resources.FsActionParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.knime.bigdata.filehandling.knox.rest.ContentSummary;
import org.knime.bigdata.filehandling.knox.rest.FileStatus;
import org.knime.bigdata.filehandling.knox.rest.KnoxHDFSClient;
import org.knime.bigdata.filehandling.knox.rest.WebHDFSAPI;
import org.knime.filehandling.core.connections.base.BaseFileSystemProvider;
import org.knime.filehandling.core.connections.base.attributes.BaseFileAttributes;
import org.knime.filehandling.core.connections.base.attributes.BasePrincipal;

/**
 * {@link KnoxHdfsFileSystem} file system provider.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
class KnoxHdfsFileSystemProvider extends BaseFileSystemProvider<KnoxHdfsPath, KnoxHdfsFileSystem> {

    @SuppressWarnings("resource")
    private WebHDFSAPI getClient() {
        return getFileSystemInternal().getClient();
    }

    @Override
    protected SeekableByteChannel newByteChannelInternal(final KnoxHdfsPath path, final Set<? extends OpenOption> options,
        final FileAttribute<?>... attrs) throws IOException {

        return new KnoxHdfsSeekableByteChannel(path, options);
    }

    @Override
    protected void moveInternal(final KnoxHdfsPath source, final KnoxHdfsPath target, final CopyOption... options)
        throws IOException {

        // remove target if required
        if (existsCached(target) && ArrayUtils.contains(options, StandardCopyOption.REPLACE_EXISTING)) {
            delete(target);
        }

        try {
            if (!getClient().rename(source.toString(), PutOpParam.Op.RENAME, target.toString())) {
                throw new IOException("Failed to move '" + source + "' to '" + target + "' (hadoop returns false).");
            }
        } catch (final Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e, source, target);
        }
    }

    @Override
    protected void copyInternal(final KnoxHdfsPath source, final KnoxHdfsPath target, final CopyOption... options)
        throws IOException {

        // remove target if required
        if (existsCached(target) && ArrayUtils.contains(options, StandardCopyOption.REPLACE_EXISTING)) {
            delete(target);
        }

        try {

            if (Files.isDirectory(source)) {
                createDirectoryInternal(target);
            } else {
                // Note: HDFS does not support copy and therefore the file will be downloaded and uploaded again.
                copyFile(source, target);
            }
        } catch (final Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e, source, target);
        }
    }

    @SuppressWarnings("resource")
    private void copyFile(final KnoxHdfsPath source, final KnoxHdfsPath target) throws IOException {
        final String sourcePath = source.getURICompatiblePath();
        final KnoxHdfsFileSystem fs = target.getFileSystem();
        final String targetPath = target.getURICompatiblePath();

        try (final InputStream in = KnoxHDFSClient.openFile(getClient(), sourcePath);
                final OutputStream out = fs.createFile(targetPath, false)) {
            IOUtils.copy(in, out);
        }
    }

    /**
     * Validate if the given path is an empty directory using the content summary instead of listing all possible files.
     *
     * @return {@code true} if the given path contains at least one child file or directory, {@code false} otherwise
     */
    @Override
    protected boolean isNonEmptyDirectory(final KnoxHdfsPath path) throws IOException {
        try  {
            // The summary includes the target in the file count, if it is a file
            // or the directory in the directory count if it is a directory.
            final ContentSummary summary = getClient().getContentSummary(path.toString(), GetOpParam.Op.GETCONTENTSUMMARY);
            return summary.fileCount + summary.directoryCount > 1;
        } catch (FileNotFoundException e) { // NOSONAR
            return false;
        }
    }

    @Override
    protected InputStream newInputStreamInternal(final KnoxHdfsPath path, final OpenOption... options) throws IOException {
        try {
            return KnoxHDFSClient.openFile(getClient(), path.toString());
        } catch (final Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e, path, null);
        }
    }

    @SuppressWarnings("resource")
    @Override
    protected OutputStream newOutputStreamInternal(final KnoxHdfsPath path, final OpenOption... options) throws IOException {
        try {
            if (ArrayUtils.contains(options, StandardOpenOption.APPEND)) {
                return path.getFileSystem().appendFile(path.toString());
            } else {
                final boolean overwrite = ArrayUtils.contains(options, StandardOpenOption.TRUNCATE_EXISTING);
                return path.getFileSystem().createFile(path.toString(), overwrite);
            }
        } catch (final Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e, path, null);
        }
    }

    @Override
    protected Iterator<KnoxHdfsPath> createPathIterator(final KnoxHdfsPath path, final Filter<? super java.nio.file.Path> filter) throws IOException {
        return new KnoxHdfsPathIterator(path, listFiles(path), filter);
    }

    FileStatus[] listFiles(final KnoxHdfsPath path) throws IOException {
        try {
            final FileStatus[] files = getClient().listStatus(path.toString(), GetOpParam.Op.LISTSTATUS).fileStatus;
            return files != null ? files : new FileStatus[0];
        } catch (final Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e, path, null);
        }
    }

    @Override
    protected void createDirectoryInternal(final KnoxHdfsPath path, final FileAttribute<?>... attrs) throws IOException {
        try {
            getClient().mkdirs(path.toString(), PutOpParam.Op.MKDIRS);
        } catch (final Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e, path, null);
        }
    }

    @Override
    protected boolean exists(final KnoxHdfsPath path) throws IOException {
        try {
            return getFileStatus(path) != null;
        } catch (final Exception e) { // NOSONAR
            final IOException mapped = ExceptionMapper.mapException(e, path, null);
            if (mapped instanceof NoSuchFileException) {
                return false;
            } else {
                throw mapped;
            }
        }
    }

    @Override
    protected BaseFileAttributes fetchAttributesInternal(final KnoxHdfsPath path, final Class<?> type) throws IOException {
        try {
            return toBaseFileAttributes(path, getFileStatus(path));
        } catch (final Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e, path, null);
        }
    }

    /**
     * Convert given {@link FileStatus} into #{@link BaseFileAttributes}.
     *
     * @param path path
     * @param status attributes to convert
     * @return converted attributes
     */
    protected static BaseFileAttributes toBaseFileAttributes(final KnoxHdfsPath path, final FileStatus status) {
        final FsPermission perms = new FsPermission(Short.parseShort(status.permission, 8));
        return new BaseFileAttributes(
            status.isFile(), //
            path, //
            FileTime.fromMillis(status.modificationTime), //
            FileTime.fromMillis(status.accessTime), //
            FileTime.fromMillis(status.modificationTime), // creation time
            status.length, //
            status.isSymlink(), //
            false, // is other
            new BasePrincipal(status.owner), //
            new BasePrincipal(status.group), //
            toPosixFilePermissions(perms));
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

    private FileStatus getFileStatus(final KnoxHdfsPath path) throws IOException {
        return getClient().getFileStatus(path.toString(), GetOpParam.Op.GETFILESTATUS);
    }

    @Override
    protected void checkAccessInternal(final KnoxHdfsPath path, final AccessMode... modes) throws IOException {
        try {
            for (final AccessMode mode : modes) {
                final FsActionParam fsAction = new FsActionParam(mapAccessModeToFsAction(mode));
                getClient().checkAccess(path.toString(), GetOpParam.Op.CHECKACCESS, fsAction.getValue());
            }
        } catch (final Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e, path, null);
        }
    }

    private static FsAction mapAccessModeToFsAction(final AccessMode mode) {
        switch (mode) {
            case READ: return FsAction.READ;
            case WRITE: return FsAction.WRITE;
            case EXECUTE: return FsAction.EXECUTE;
        }
        throw new IllegalArgumentException("Unknown access mode: " + mode);
    }

    @Override
    protected void deleteInternal(final KnoxHdfsPath path) throws IOException {
        try {
            final boolean recursive = false;
            getClient().delete(path.toString(), DeleteOpParam.Op.DELETE, recursive);
        } catch (final Exception e) { // NOSONAR
            throw ExceptionMapper.mapException(e, path, null);
        }
    }
}
