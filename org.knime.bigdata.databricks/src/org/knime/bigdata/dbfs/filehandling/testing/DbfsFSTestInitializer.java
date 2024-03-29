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
 *   2020-11-01 (Alexander Bondaletov): created
 */
package org.knime.bigdata.dbfs.filehandling.testing;

import java.io.Closeable;
import java.io.IOException;
import java.util.Base64;

import org.knime.bigdata.databricks.rest.dbfs.AddBlock;
import org.knime.bigdata.databricks.rest.dbfs.Close;
import org.knime.bigdata.databricks.rest.dbfs.Create;
import org.knime.bigdata.databricks.rest.dbfs.DBFSAPI;
import org.knime.bigdata.databricks.rest.dbfs.Delete;
import org.knime.bigdata.databricks.rest.dbfs.FileHandle;
import org.knime.bigdata.databricks.rest.dbfs.Mkdir;
import org.knime.bigdata.dbfs.filehandling.fs.DbfsFSConnection;
import org.knime.bigdata.dbfs.filehandling.fs.DbfsFileSystem;
import org.knime.bigdata.dbfs.filehandling.fs.DbfsPath;
import org.knime.core.util.ThreadLocalHTTPAuthenticator;
import org.knime.filehandling.core.testing.DefaultFSTestInitializer;

/**
 * Databricks DBFS test initializer.
 *
 * @author Alexander Bondaletov
 */
public class DbfsFSTestInitializer extends DefaultFSTestInitializer<DbfsPath, DbfsFileSystem> {

    private final DBFSAPI m_client;

    /**
     * Creates test initializer.
     *
     * @param fsConnection FS connection.
     */
    protected DbfsFSTestInitializer(final DbfsFSConnection fsConnection) {
        super(fsConnection);
        m_client = getFileSystem().getClient();
    }

    @Override
    public DbfsPath createFileWithContent(final String content, final String... pathComponents) throws IOException {
        DbfsPath path = makePath(pathComponents);
        try (final Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            FileHandle handle = m_client.create(Create.create(path.toString(), true));
            m_client.addBlock(AddBlock.create(handle.handle, Base64.getEncoder().encodeToString(content.getBytes())));//NOSONAR
            m_client.close(Close.create(handle.handle));
        }
        return path;
    }

    @Override
    protected void beforeTestCaseInternal() throws IOException {
        DbfsPath scratchDir = getTestCaseScratchDir();
        m_client.mkdirs(Mkdir.create(scratchDir.toString()));
    }

    @Override
    protected void afterTestCaseInternal() throws IOException {
        DbfsPath scratchDir = getTestCaseScratchDir();
        m_client.delete(Delete.create(scratchDir.toString(), true));
    }

}
