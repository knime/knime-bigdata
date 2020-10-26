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
package org.knime.bigdata.hadoop.filesystem.testing;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import org.knime.bigdata.hadoop.filehandling.fs.HdfsFileSystem;
import org.knime.bigdata.hadoop.filehandling.fs.HdfsPath;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSFiles;
import org.knime.filehandling.core.connections.local.LocalFileSystem;
import org.knime.filehandling.core.testing.DefaultFSTestInitializer;

/**
 * HDFS wrapper wrapper test initializer.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class LocalHdfsTestInitializer extends DefaultFSTestInitializer<HdfsPath, HdfsFileSystem> {

    private final LocalFileSystem m_localFileSystem;

    /**
     * Default constructor
     *
     * @param fsConnection {@link FSConnection} to use
     */
    public LocalHdfsTestInitializer(final LocalHdfsFSConnection fsConnection) {
        super(fsConnection);
        m_localFileSystem = fsConnection.getLocalFileSystem();
    }

    @Override
    public HdfsPath createFileWithContent(final String content, final String... pathComponents) throws IOException {
        final HdfsPath file = makePath(pathComponents);

        Files.createDirectories(file.getParent());
        Files.write(file, content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING);

        return file;
    }

    @Override
    protected void beforeTestCaseInternal() throws IOException {
        final String scratchDir = getTestCaseScratchDir().toString();
        Files.createDirectories(m_localFileSystem.getPath(scratchDir));
    }

    @Override
    protected void afterTestCaseInternal() throws IOException {
        final String scratchDir = getTestCaseScratchDir().toString();
        FSFiles.deleteRecursively(m_localFileSystem.getPath(scratchDir));
    }
}
