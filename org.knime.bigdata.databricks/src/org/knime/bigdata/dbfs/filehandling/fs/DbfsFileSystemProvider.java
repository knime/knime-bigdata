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
 *
 * History
 *   2020-10-14 (Alexander Bondaletov): created
 */
package org.knime.bigdata.dbfs.filehandling.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.knime.bigdata.databricks.rest.dbfs.DBFSAPI;
import org.knime.bigdata.databricks.rest.dbfs.Delete;
import org.knime.bigdata.databricks.rest.dbfs.FileInfo;
import org.knime.bigdata.databricks.rest.dbfs.Mkdir;
import org.knime.bigdata.databricks.rest.dbfs.Move;
import org.knime.bigdata.dbfs.filehandler.DBFSInputStream;
import org.knime.bigdata.dbfs.filehandler.DBFSOutputStream;
import org.knime.filehandling.core.connections.base.BaseFileSystemProvider;
import org.knime.filehandling.core.connections.base.attributes.BaseFileAttributes;

/**
 * File system provider for the {@link DbfsFileSystem}.
 *
 * @author Alexander Bondaletov
 */
class DbfsFileSystemProvider extends BaseFileSystemProvider<DbfsPath, DbfsFileSystem> {

    @Override
    protected SeekableByteChannel newByteChannelInternal(final DbfsPath path, final Set<? extends OpenOption> options,
            final FileAttribute<?>... attrs) throws IOException {
        return new DbfsSeekableByteChannel(path, options);
    }

    @SuppressWarnings("resource")
    @Override
    protected void moveInternal(final DbfsPath source, final DbfsPath target, final CopyOption... options)
            throws IOException {
        if (existsCached(target)) {
            delete(target);
        }

        DBFSAPI client = source.getFileSystem().getClient();
        client.move(Move.create(source.toString(), target.toString()));
    }

    @Override
    protected void copyInternal(final DbfsPath source, final DbfsPath target, final CopyOption... options)
            throws IOException {
        if (Files.isDirectory(source)) {
            if (!existsCached(target)) {
                createDirectory(target);
            }
        } else {
            //DBFS API doesn't have a 'copy' method so we have to do it this way
            try (InputStream in = newInputStream(source)) {
                Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    @SuppressWarnings("resource")
    @Override
    protected InputStream newInputStreamInternal(final DbfsPath path, final OpenOption... options) throws IOException {
        return new DBFSInputStream(path.toString(), path.getFileSystem().getClient());
    }

    @SuppressWarnings("resource")
    @Override
    protected OutputStream newOutputStreamInternal(final DbfsPath path, final OpenOption... options) throws IOException {
        final Set<OpenOption> opts = new HashSet<>(Arrays.asList(options));

        if (opts.contains(StandardOpenOption.APPEND)) {
            //Fallback to TempFileSeekableByteChannel because DBFSOutputStream doesn't support the APPEND mode
            return Channels.newOutputStream(newByteChannel(path, opts));
        } else {
            return new DBFSOutputStream(path.toString(), path.getFileSystem().getClient(), true);
        }
    }

    @Override
    protected Iterator<DbfsPath> createPathIterator(final DbfsPath dir, final Filter<? super Path> filter)
            throws IOException {
        return new DbfsPathIterator(dir, filter);
    }

    @SuppressWarnings("resource")
    @Override
    protected void createDirectoryInternal(final DbfsPath dir, final FileAttribute<?>... attrs) throws IOException {
        dir.getFileSystem().getClient().mkdirs(Mkdir.create(dir.toString()));
    }

    @SuppressWarnings("resource")
    @Override
    protected BaseFileAttributes fetchAttributesInternal(final DbfsPath path, final Class<?> type) throws IOException {
        try {
            FileInfo info = path.getFileSystem().getClient().getStatus(path.toString());
            return createBaseFileAttrs(info, path);
        } catch (FileNotFoundException e) {
            throw toNSFException(e, path.toString());
        }
    }

    private static NoSuchFileException toNSFException(final FileNotFoundException cause, final String path) {
        NoSuchFileException nsfe = new NoSuchFileException(path);
        nsfe.initCause(cause);
        return nsfe;
    }

    /**
     * Creates {@link BaseFileAttributes} instance from a given {@link FileInfo} for a given path.
     *
     * @param info The {@link FileInfo} object instance.
     * @param path The path.
     * @return The attributes.
     */
    public static BaseFileAttributes createBaseFileAttrs(final FileInfo info, final DbfsPath path) {
        FileTime mtime = FileTime.fromMillis(info.modification_time);
        return new BaseFileAttributes(!info.is_dir, path, mtime, mtime, mtime, info.file_size, false, false, null);
    }

    @Override
    protected void checkAccessInternal(final DbfsPath path, final AccessMode... modes) throws IOException {
        // nothing to do here
    }

    @SuppressWarnings("resource")
    @Override
    protected void deleteInternal(final DbfsPath path) throws IOException {
        DBFSAPI client = path.getFileSystem().getClient();
        client.delete(Delete.create(path.toString(), false));
    }

    @Override
    protected boolean isHiddenInternal(final DbfsPath path) throws IOException {
        if (path.isRoot()) {
            return false;
        } else {
            final String name = path.getFileName().toString();
            return name.startsWith(".") || name.startsWith("_");
        }
    }

}
