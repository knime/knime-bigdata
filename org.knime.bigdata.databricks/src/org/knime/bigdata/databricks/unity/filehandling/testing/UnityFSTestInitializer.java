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
package org.knime.bigdata.databricks.unity.filehandling.testing;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.knime.bigdata.databricks.rest.files.FilesAPI;
import org.knime.bigdata.databricks.unity.filehandling.fs.UnityFSConnection;
import org.knime.bigdata.databricks.unity.filehandling.fs.UnityFileSystem;
import org.knime.bigdata.databricks.unity.filehandling.fs.UnityPath;
import org.knime.filehandling.core.testing.DefaultFSTestInitializer;

import jakarta.ws.rs.ClientErrorException;

/**
 * Databricks Unity Filesystem test initializer.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
public class UnityFSTestInitializer extends DefaultFSTestInitializer<UnityPath, UnityFileSystem> {

    private final FilesAPI m_client;

    /**
     * Creates test initializer.
     *
     * @param fsConnection FS connection.
     */
    protected UnityFSTestInitializer(final UnityFSConnection fsConnection) {
        super(fsConnection);
        m_client = getFileSystem().getFilesClient();
    }

    @Override
    public UnityPath createFileWithContent(final String content, final String... pathComponents) throws IOException {
        final UnityPath path = makePath(pathComponents);

        try (final InputStream is = IOUtils.toInputStream(content, StandardCharsets.UTF_8)) {
            m_client.upload(path.getURICompatiblePath(), true, is);
            return path;
        } catch (final ClientErrorException e) {
            final IOException ioe = new FileSystemException(path.toString(), null, e.getMessage());
            ioe.initCause(e);
            throw ioe;
        }
    }

    @Override
    protected void beforeTestCaseInternal() throws IOException {
        final UnityPath scratchDir = getTestCaseScratchDir();
        try {
            m_client.mkdirs(scratchDir.getURICompatiblePath());
        } catch (final ClientErrorException e) {
            final IOException ioe = new FileSystemException(scratchDir.toString(), null, e.getMessage());
            ioe.initCause(e);
            throw ioe;
        }
    }

    @Override
    protected void afterTestCaseInternal() throws IOException {
        final UnityPath scratchDir = getTestCaseScratchDir();

        try (final Stream<Path> files = Files.walk(scratchDir)) {
            for (final Path path : files.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
                deleteFileOrDirectory((UnityPath)path);
            }
        }
    }

    private void deleteFileOrDirectory(final UnityPath path) throws IOException {
        try {
            if (Files.isDirectory(path)) {
                m_client.deleteDirectory(path.getURICompatiblePath());
            } else {
                m_client.deleteFile(path.getURICompatiblePath());
            }
        } catch (final ClientErrorException e) {
            final IOException ioe = new FileSystemException(path.toString(), null, e.getMessage());
            ioe.initCause(e);
            throw ioe;
        }
    }

}
