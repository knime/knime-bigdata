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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.OpenOption;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.knime.filehandling.core.connections.base.TempFileSeekableByteChannel;
import org.knime.filehandling.core.defaultnodesettings.ExceptionUtil;


/**
 * HDFS implementation of the {@link TempFileSeekableByteChannel}.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class HdfsSeekableByteChannel extends TempFileSeekableByteChannel<HdfsPath> {

    /**
     * Constructs an {@link TempFileSeekableByteChannel} for HDFS.
     *
     * @param file the file for the channel
     * @param options the open options
     * @throws IOException if an I/O Error occurred
     */
    public HdfsSeekableByteChannel(final HdfsPath file, final Set<? extends OpenOption> options) throws IOException {
        super(file, options);
    }

    @SuppressWarnings("resource")
    @Override
    public void copyFromRemote(final HdfsPath remoteFile, final java.nio.file.Path tempFile) throws IOException {
        try {
            final Path src = remoteFile.toHadoopPath();
            final Path dst = new Path(tempFile.toUri());
            final boolean deleteSource = false;
            // use the raw local file system instead of the checksum local file system to avoid crc errors
            // (the temporary file might be modified without using the hadoop api's and without updating the checksum)
            final boolean useRawLocalFileSystem = true;
            remoteFile.getFileSystem().getHadoopFileSystem().copyToLocalFile(deleteSource, src, dst, useRawLocalFileSystem);
        } catch (final AccessControlException e) { // NOSONAR
            throw new AccessDeniedException(remoteFile.toString());
        } catch (final FileNotFoundException e) { // NOSONAR
            // source file does not exists
        } catch (final Exception e) { // NOSONAR
            throw ExceptionUtil.wrapAsIOException(e);
        }
    }

    @SuppressWarnings("resource")
    @Override
    public void copyToRemote(final HdfsPath remoteFile, final java.nio.file.Path tempFile) throws IOException {
        try {
            final Path src = new Path(tempFile.toUri());
            final Path dst = remoteFile.toHadoopPath();
            final boolean deleteSource = false;
            final boolean overwrite = true;
            remoteFile.getFileSystem().getHadoopFileSystem().copyFromLocalFile(deleteSource, overwrite, src, dst);
        } catch (final AccessControlException e) { // NOSONAR
            throw new AccessDeniedException(remoteFile.toString());
        } catch (final Exception e) { // NOSONAR
            throw ExceptionUtil.wrapAsIOException(e);
        }
    }
}
