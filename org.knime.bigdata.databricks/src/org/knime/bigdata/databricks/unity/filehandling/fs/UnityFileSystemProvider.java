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
package org.knime.bigdata.databricks.unity.filehandling.fs;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.knime.bigdata.databricks.rest.catalog.CatalogAPI;
import org.knime.bigdata.databricks.rest.catalog.CatalogInfo;
import org.knime.bigdata.databricks.rest.catalog.CatalogSchemaInfo;
import org.knime.bigdata.databricks.rest.catalog.CatalogVolumesInfo;
import org.knime.bigdata.databricks.rest.catalog.MetastoreSummary;
import org.knime.bigdata.databricks.rest.files.FileInfo;
import org.knime.bigdata.databricks.rest.files.FilesAPI;
import org.knime.filehandling.core.connections.base.BaseFileSystemProvider;
import org.knime.filehandling.core.connections.base.attributes.BaseFileAttributes;

import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.core.Response;

/**
 * File system provider for the {@link UnityFileSystem}.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
class UnityFileSystemProvider extends BaseFileSystemProvider<UnityPath, UnityFileSystem> {

    @Override
    protected SeekableByteChannel newByteChannelInternal(final UnityPath path, final Set<? extends OpenOption> options,
            final FileAttribute<?>... attrs) throws IOException {
        return new UnitySeekableByteChannel(path, options);
    }

    @Override
    protected void copyInternal(final UnityPath source, final UnityPath target, final CopyOption... options)
            throws IOException {

        ensureInsideVolume(target);

        if (Files.isDirectory(source)) {
            if (!existsCached(target)) {
                createDirectory(target);
            }
        } else {
            // Files API doesn't have a copy endpoint
            try (InputStream in = newInputStream(source)) {
                Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    private static void ensureInsideVolume(final UnityPath path) throws IOException {
        if (!path.isInsideVolume()) {
            throw new AccessDeniedException(path.toString(), null,
                "Only files and directories inside a volume supported.");
        }
    }

    @SuppressWarnings("resource")
    @Override
    protected InputStream newInputStreamInternal(final UnityPath path, final OpenOption... options) throws IOException {
        ensureInsideVolume(path);

        try {
            return path.getFileSystem().getFilesClient().download(path.getURICompatiblePath());
        } catch (final ClientErrorException e) {
            throw toIOException(path, e);
        }
    }

    @SuppressWarnings("resource")
    @Override
    protected OutputStream newOutputStreamInternal(final UnityPath path, final OpenOption... options)
        throws IOException {

        final Set<OpenOption> opts = new HashSet<>(Arrays.asList(options));
        ensureInsideVolume(path);

        try {
            if (opts.contains(StandardOpenOption.APPEND)) {
                // Fallback to TempFileSeekableByteChannel because UnityFileOutputStream doesn't support the APPEND mode
                return Channels.newOutputStream(newByteChannel(path, opts));
            } else {
                return new UnityFileOutputStream(path.getURICompatiblePath(), path.getFileSystem().getFilesClient(),
                    true);
            }
        } catch (final ClientErrorException e) {
            throw toIOException(path, e);
        }
    }

    @Override
    protected Iterator<UnityPath> createPathIterator(final UnityPath path, final Filter<? super Path> filter)
            throws IOException {

        if (path.isRoot()) {
            return new UnityCatalogIterator(path, filter);
        } else if (path.isCatalog()) {
            return new UnitySchemaIterator(path, filter);
        } else if (path.isCatalogSchema()) {
            return new UnityVolumesIterator(path, filter);
        } else {
            return new UnityPathIterator(path, filter);
        }
    }

    @SuppressWarnings("resource")
    @Override
    protected void createDirectoryInternal(final UnityPath path, final FileAttribute<?>... attrs) throws IOException {
        ensureInsideVolume(path);

        try {
            path.getFileSystem().getFilesClient().mkdirs(path.getURICompatiblePath());
        } catch (final ClientErrorException e) {
            throw toIOException(path, e);
        }
    }

    @Override
    protected BaseFileAttributes fetchAttributesInternal(final UnityPath path, final Class<?> type) throws IOException {
        try {
            if (path.isRoot()) {
                return fetchMetastoreAttributes(path);
            } else if (path.isCatalog()) {
                return fetchCatalogAttributes(path);
            } else if (path.isCatalogSchema()) {
                return fetchSchemaAttributes(path);
            } else if (path.isCatalogVolume()) {
                return fetchVolumeAttributes(path);
            } else {
                return fetchFileOrDirectoryAttributes(path);
            }
        } catch (final ClientErrorException e) {
            throw toIOException(path, e);
        }
    }

    @SuppressWarnings("resource")
    private static BaseFileAttributes fetchMetastoreAttributes(final UnityPath path) throws IOException {
        final CatalogAPI client = path.getFileSystem().getCatalogClient();
        return createBaseFileAttrs(client.getCurrentMetastore(), path);
    }

    @SuppressWarnings("resource")
    private static BaseFileAttributes fetchCatalogAttributes(final UnityPath path) throws IOException {
        final CatalogAPI client = path.getFileSystem().getCatalogClient();
        final String catalog = path.getName(0).toString();
        return createBaseFileAttrs(client.getCatalogMetadata(catalog), path);
    }

    @SuppressWarnings("resource")
    private static BaseFileAttributes fetchSchemaAttributes(final UnityPath path) throws IOException {
        final CatalogAPI client = path.getFileSystem().getCatalogClient();
        final String catalog = path.getName(0).toString();
        final String schema = path.getName(1).toString();
        return createBaseFileAttrs(client.getSchemaMetadata(catalog, schema), path);
    }

    @SuppressWarnings("resource")
    private static BaseFileAttributes fetchVolumeAttributes(final UnityPath path) throws IOException {
        final CatalogAPI client = path.getFileSystem().getCatalogClient();
        final String catalog = path.getName(0).toString();
        final String schema = path.getName(1).toString();
        final String volume = path.getName(2).toString();
        return createBaseFileAttrs(client.getVolumeMetadata(catalog, schema, volume), path);
    }

    private static BaseFileAttributes fetchFileOrDirectoryAttributes(final UnityPath path) throws IOException {
        try {
            return fetchFileAttributes(path);
        } catch (final NotFoundException e) { // NOSONAR not a file
            // in case the file was not found, it might be a directory instead
            return fetchDirectoryAttributes(path);
        }
    }

    private static BaseFileAttributes fetchFileAttributes(final UnityPath path) throws IOException {
        try (final Response res = path.getFileSystem().getFilesClient().getFileMetadata(path.getURICompatiblePath())) {
            final boolean isFile = true;
            final FileTime mtime = parseHttpDate(res.getHeaderString("last-modified"));
            final int file_size = Integer.parseInt(res.getHeaderString("content-length"));
            return new BaseFileAttributes(isFile, path, mtime, mtime, mtime, file_size, false, false, null);
        }
    }

    private static FileTime parseHttpDate(final String timestamp) {
        final Instant instant = ZonedDateTime.parse(timestamp, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
        return FileTime.from(instant);
    }

    @SuppressWarnings("resource")
    private static BaseFileAttributes fetchDirectoryAttributes(final UnityPath path) throws IOException {
        // if the following request does not fail, the path exists and is an directory
        path.getFileSystem().getFilesClient().getDirectoryMetadata(path.getURICompatiblePath());

        final boolean isFile = false;
        final FileTime mtime = null;
        final int file_size = 0;
        return new BaseFileAttributes(isFile, path, mtime, mtime, mtime, file_size, false, false, null);
    }

    static BaseFileAttributes createBaseFileAttrs(final MetastoreSummary info, final UnityPath path) {
        final FileTime ctime = FileTime.fromMillis(info.createdAt);
        final FileTime mtime = FileTime.fromMillis(info.updatedAt);
        return new BaseFileAttributes(false, path, mtime, mtime, ctime, 0, false, false, null);
    }

    static BaseFileAttributes createBaseFileAttrs(final CatalogInfo info, final UnityPath path) {
        final FileTime ctime = FileTime.fromMillis(info.createdAt);
        final FileTime mtime = FileTime.fromMillis(info.updatedAt);
        return new BaseFileAttributes(false, path, mtime, mtime, ctime, 0, false, false, null);
    }

    static BaseFileAttributes createBaseFileAttrs(final CatalogSchemaInfo info, final UnityPath path) {
        final FileTime ctime = FileTime.fromMillis(info.createdAt);
        final FileTime mtime = FileTime.fromMillis(info.updatedAt);
        return new BaseFileAttributes(false, path, mtime, mtime, ctime, 0, false, false, null);
    }

    static BaseFileAttributes createBaseFileAttrs(final CatalogVolumesInfo info, final UnityPath path) {
        final FileTime ctime = FileTime.fromMillis(info.createdAt);
        final FileTime mtime = FileTime.fromMillis(info.updatedAt);
        return new BaseFileAttributes(false, path, mtime, mtime, ctime, 0, false, false, null);
    }

    static BaseFileAttributes createBaseFileAttrs(final FileInfo info, final UnityPath path) {
        FileTime mtime = FileTime.fromMillis(info.modification_time);
        return new BaseFileAttributes(!info.is_dir, path, mtime, mtime, mtime, info.file_size, false, false, null);
    }

    @Override
    protected void checkAccessInternal(final UnityPath path, final AccessMode... modes) throws IOException {
        // nothing to do here
    }

    @SuppressWarnings("resource")
    @Override
    protected void deleteInternal(final UnityPath path) throws IOException {
        ensureInsideVolume(path);

        try {
            final FilesAPI client = path.getFileSystem().getFilesClient();
            if (Files.isDirectory(path)) {
                client.deleteDirectory(path.getURICompatiblePath());
            } else {
                client.deleteFile(path.getURICompatiblePath());
            }
        } catch (final ClientErrorException e) {
            throw toIOException(path, e);
        }
    }

    @Override
    protected boolean isHiddenInternal(final UnityPath path) throws IOException {
        if (path.isRoot()) {
            return false;
        } else {
            final String name = path.getFileName().toString();
            return name.startsWith(".") || name.startsWith("_");
        }
    }

    static IOException toIOException(final UnityPath path, final ClientErrorException ex) {
        final IOException result;

        if (ex instanceof NotAuthorizedException || ex instanceof ForbiddenException) {
            result = new AccessDeniedException(path.toString(), null, ex.getMessage());
        } else if (ex instanceof NotFoundException) {
            result = new NoSuchFileException(path.toString());
        } else {
            result = new FileSystemException(path.toString(), null, ex.getMessage());
        }

        result.initCause(ex);
        return result;
    }

}
