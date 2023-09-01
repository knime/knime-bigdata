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
 *   Oct 30, 2020 (Bjoern Lohrmann, KNIME GmbH): created
 */
package org.knime.bigdata.filehandling.knox.rest;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.resources.GetOpParam.Op;
import org.knime.core.util.ThreadLocalHTTPAuthenticator;

import jakarta.ws.rs.core.Response;

/**
 * Wrapper class for {@link WebHDFSAPI} that suppresses authentication popups.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class WebHDFSAPIWrapper implements WebHDFSAPI {

    private final WebHDFSAPI m_api;

    /**
     * Creates a new instance.
     *
     * @param api The {@link WebHDFSAPI} instance to wrap.
     */
    public WebHDFSAPIWrapper(final WebHDFSAPI api) {
        m_api = api;
    }

    private interface Invoker<T> {
        T invoke() throws IOException;
    }

    private static <T> T invoke(final Invoker<T> invoker) throws IOException {
        try (Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            return invoker.invoke();
        }
    }

    private static final String percentEncodePath(final String unencodedPath) {
        try {
            return new URI("dummy", "dummy", unencodedPath, null, null).getRawPath();
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    private static final String percentEncodeQueryValue(final String value) {
        try {
            return new URI("dummy", "dummy", "/", value, null).getRawQuery();
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    @Override
    public FileStatus getFileStatus(final String path, final Op op) throws IOException {
        return invoke(() -> m_api.getFileStatus(percentEncodePath(path), op));
    }

    @Override
    public FileStatuses listStatus(final String path, final Op op) throws IOException {
        return invoke(() -> m_api.listStatus(percentEncodePath(path), op));
    }

    @Override
    public ContentSummary getContentSummary(final String path, final Op op) throws IOException {
        return invoke(() -> m_api.getContentSummary(percentEncodePath(path), op));
    }

    @Override
    public void checkAccess(final String path, final Op op, final String action) throws IOException {
        invoke(() -> {
            m_api.checkAccess(percentEncodePath(path), op, action);
            return null;
        });
    }

    @Override
    public Path getHomeDirectory(final Op op) throws IOException {
        return invoke(() -> m_api.getHomeDirectory(op));
    }

    @Override
    public boolean mkdirs(final String path, final org.apache.hadoop.hdfs.web.resources.PutOpParam.Op op)
        throws IOException {
        return invoke(() -> m_api.mkdirs(percentEncodePath(path), op));
    }

    @Override
    public boolean rename(final String path, final org.apache.hadoop.hdfs.web.resources.PutOpParam.Op op,
        final String destination) throws IOException {
        return invoke(() -> m_api.rename(percentEncodePath(path), op, percentEncodeQueryValue(destination)));
    }

    @Override
    public void setPermission(final String path, final org.apache.hadoop.hdfs.web.resources.PutOpParam.Op op,
        final short permission) throws IOException {
        invoke(() -> {
            m_api.setPermission(percentEncodePath(path), op, permission);
            return null;
        });
    }

    @Override
    public boolean delete(final String path, final org.apache.hadoop.hdfs.web.resources.DeleteOpParam.Op op,
        final boolean recursive) throws IOException {
        return invoke(() -> m_api.delete(percentEncodePath(path), op, recursive));
    }

    @Override
    public Response open(final String path, final Op op, final long buffersize) {
        try {
            return invoke(() -> m_api.open(percentEncodePath(path), op, buffersize));
        } catch (IOException ex) { // NOSONAR never happens
            return null;
        }
    }

    @Override
    public Response create(final String path, final org.apache.hadoop.hdfs.web.resources.PutOpParam.Op op,
        final long buffersize, final boolean overwrite) {
        try {
            return invoke(() -> m_api.create(percentEncodePath(path), op, buffersize, overwrite));
        } catch (IOException ex) { // NOSONAR never happens
            return null;
        }
    }

    @Override
    public Response append(final String path, final org.apache.hadoop.hdfs.web.resources.PostOpParam.Op op,
        final long buffersize) {
        try {
            return invoke(() -> m_api.append(percentEncodePath(path), op, buffersize));
        } catch (IOException ex) { // NOSONAR never happens
            return null;
        }
    }

    WebHDFSAPI getWrappedWebHDFsAPI() {
        return m_api;
    }

    @Override
    public Response setTimes(final String path, final org.apache.hadoop.hdfs.web.resources.PutOpParam.Op op,
        final long mtime, final long atime) throws IOException {
        return invoke(() -> m_api.setTimes(path, op, mtime, atime));
    }

    @Override
    public Response setOwner(final String path, final org.apache.hadoop.hdfs.web.resources.PutOpParam.Op op,
        final String user) throws IOException {
        return invoke(() -> m_api.setOwner(path, op, user));
    }

    @Override
    public Response setGroup(final String path, final org.apache.hadoop.hdfs.web.resources.PutOpParam.Op op,
        final String group) throws IOException {
        return invoke(() -> m_api.setGroup(path, op, group));
    }

    WebHDFSAPI getWrappedApiClient() {
        return m_api;
    }

}
