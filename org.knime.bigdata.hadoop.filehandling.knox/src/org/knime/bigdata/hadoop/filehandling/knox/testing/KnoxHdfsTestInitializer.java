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
package org.knime.bigdata.hadoop.filehandling.knox.testing;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hdfs.web.resources.DeleteOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.knime.bigdata.filehandling.knox.rest.KnoxHDFSClient;
import org.knime.bigdata.filehandling.knox.rest.WebHDFSAPI;
import org.knime.bigdata.hadoop.filehandling.knox.fs.KnoxHdfsFSConnection;
import org.knime.bigdata.hadoop.filehandling.knox.fs.KnoxHdfsFileSystem;
import org.knime.bigdata.hadoop.filehandling.knox.fs.KnoxHdfsPath;
import org.knime.core.util.ThreadLocalHTTPAuthenticator;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.testing.DefaultFSTestInitializer;

/**
 * WebHDFS via KNOX test initializer.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
class KnoxHdfsTestInitializer extends DefaultFSTestInitializer<KnoxHdfsPath, KnoxHdfsFileSystem> {

    private final WebHDFSAPI m_client;
    private final ExecutorService m_uploadExecutor;

    /**
     * Default constructor
     *
     * @param fsConnection {@link FSConnection} to use
     */
    public KnoxHdfsTestInitializer(final KnoxHdfsFSConnection fsConnection) {
        super(fsConnection);
        m_client = fsConnection.getRestClient();
        m_uploadExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "knox-webhdfs-test-uploader"));
    }

    @Override
    public KnoxHdfsPath createFileWithContent(final String content, final String... pathComponents) throws IOException {
        final KnoxHdfsPath path = makePath(pathComponents);
        try (Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            m_client.mkdirs(((KnoxHdfsPath)path.getParent()).getURICompatiblePath(), PutOpParam.Op.MKDIRS);
            try (final OutputStream stream = KnoxHDFSClient.createFile(m_client, m_uploadExecutor, path.toUri().getPath(), false)) {
                stream.write(content.getBytes(StandardCharsets.UTF_8));
            }
        }
        return path;
    }

    @Override
    protected void beforeTestCaseInternal() throws IOException {
        final KnoxHdfsPath scratchDir = getTestCaseScratchDir();
        try (Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            m_client.mkdirs(scratchDir.getURICompatiblePath(), PutOpParam.Op.MKDIRS);
        }
    }

    @Override
    protected void afterTestCaseInternal() throws IOException {
        final KnoxHdfsPath scratchDir = getTestCaseScratchDir();
        try (Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            final boolean recursive = true;
            m_client.delete(scratchDir.getURICompatiblePath(), DeleteOpParam.Op.DELETE, recursive);
        }
    }
}
