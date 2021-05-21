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
 *   2020-11-10 (Alexander Bondaletov): created
 */
package org.knime.bigdata.databricks.rest.dbfs;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.knime.core.util.ThreadLocalHTTPAuthenticator;

/**
 * Wrapper class for {@link DBFSAPI} that suppresses authentication popups and performs percent-encoding of URL query
 * params (necessary, because JAX-RS and CXF only perform application/x-www-form-urlencoded).
 *
 * @author Alexander Bondaletov
 */
public class DBFSAPIWrapper implements DBFSAPI {

    private final DBFSAPI m_api;

    /**
     * Creates new instance.
     *
     * @param api The {@link DBFSAPI} instance to be wrapped.
     *
     */
    public DBFSAPIWrapper(final DBFSAPI api) {
        m_api = api;
    }

    private static final String percentEncodeQueryValue(final String value) {
        try {
            return new URI("dummy", "dummy", "/", value, null).getRawQuery();
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    private interface Invoker<T> {
        T invoke() throws IOException;
    }

    private static <T> T invoke(final Invoker<T> invoker) throws IOException {
        try (Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            return invoker.invoke();
        }
    }

    @Override
    public FileInfo getStatus(final String path) throws IOException {

        return invoke(() -> m_api.getStatus(percentEncodeQueryValue(path)));
    }

    @Override
    public FileInfoList list(final String path) throws IOException {
        return invoke(() -> m_api.list(percentEncodeQueryValue(path)));
    }

    @Override
    public void mkdirs(final Mkdir mkdir) throws IOException {
        invoke(() -> {
            m_api.mkdirs(mkdir);
            return null;
        });
    }

    @Override
    public void move(final Move move) throws IOException {
        invoke(() -> {
            m_api.move(move);
            return null;
        });
    }

    @Override
    public void delete(final Delete delete) throws IOException {
        invoke(() -> {
            m_api.delete(delete);
            return null;
        });
    }

    @Override
    public FileBlock read(final String path, final long offset, final long length) throws IOException {
        return invoke(() -> m_api.read(percentEncodeQueryValue(path), offset, length));
    }

    @Override
    public FileHandle create(final Create create) throws IOException {
        return invoke(() -> m_api.create(create));
    }

    @Override
    public void addBlock(final AddBlock addBlock) throws IOException {
        invoke(() -> {
            m_api.addBlock(addBlock);
            return null;
        });
    }

    @Override
    public void close(final Close close) throws IOException {
        invoke(() -> {
            m_api.close(close);
            return null;
        });
    }

    /**
     * @return the underlying {@link DBFSAPI} object.
     */
    public DBFSAPI getWrappedDBFSAPI() {
        return m_api;
    }
}
