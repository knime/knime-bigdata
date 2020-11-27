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
package org.knime.bigdata.hadoop.filehandling.testing;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.knime.bigdata.hadoop.filehandling.fs.HdfsFSConnection;
import org.knime.bigdata.hadoop.filehandling.fs.HdfsFileSystem;
import org.knime.bigdata.hadoop.filehandling.fs.HdfsPath;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.testing.DefaultFSTestInitializer;

/**
 * HDFS test initializer.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class HdfsTestInitializer extends DefaultFSTestInitializer<HdfsPath, HdfsFileSystem> {

    private final FileSystem m_hadoopFileSystem;

    /**
     * Default constructor
     *
     * @param fsConnection {@link FSConnection} to use
     */
    @SuppressWarnings("resource")
    public HdfsTestInitializer(final HdfsFSConnection fsConnection) {
        super(fsConnection);
        m_hadoopFileSystem = fsConnection.getFileSystem().getHadoopFileSystem();
    }

    @Override
    public HdfsPath createFileWithContent(final String content, final String... pathComponents) throws IOException {
        final HdfsPath path = makePath(pathComponents);
        final Path hadoopPath = path.toHadoopPath();
        m_hadoopFileSystem.mkdirs(hadoopPath.getParent());
        try (final FSDataOutputStream stream = m_hadoopFileSystem.create(hadoopPath)) {
            stream.write(content.getBytes(StandardCharsets.UTF_8));
        }
        return path;
    }

    @Override
    protected void beforeTestCaseInternal() throws IOException {
        final HdfsPath scratchDir = getTestCaseScratchDir();
        m_hadoopFileSystem.mkdirs(scratchDir.toHadoopPath());
    }

    @Override
    protected void afterTestCaseInternal() throws IOException {
        final HdfsPath scratchDir = getTestCaseScratchDir();
        m_hadoopFileSystem.delete(scratchDir.toHadoopPath(), true);
    }
}
