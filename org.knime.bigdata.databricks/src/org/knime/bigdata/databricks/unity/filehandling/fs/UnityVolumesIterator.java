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
 *   2024-05-15 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.databricks.unity.filehandling.fs;

import static org.knime.bigdata.databricks.unity.filehandling.fs.UnityFileSystemProvider.toIOException;

import java.io.IOException;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.databricks.rest.catalog.CatalogAPI;
import org.knime.bigdata.databricks.rest.catalog.CatalogVolumesInfo;
import org.knime.bigdata.databricks.rest.catalog.CatalogVolumesInfoList;
import org.knime.filehandling.core.connections.base.PagedPathIterator;

import jakarta.ws.rs.ClientErrorException;

/**
 * Paged iterator to list volumes of a Unity Catalog Schema.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
public class UnityVolumesIterator extends PagedPathIterator<UnityPath> {

    private final String m_catalog;

    private final String m_schema;

    private String m_nextPage;

    /**
     * Creates iterator instance.
     *
     * @param path path to iterate.
     * @param filter {@link Filter} instance.
     * @throws IOException
     */
    UnityVolumesIterator(final UnityPath path, final Filter<? super Path> filter) throws IOException {
        super(path, filter);

        if (!path.isCatalogSchema()) {
            throw new IOException("Only catalog schema path supported in volumes listing");
        }

        m_catalog = path.getName(0).toString();
        m_schema = path.getName(1).toString();
        setFirstPage(loadNextPage()); // NOSONAR
    }

    @Override
    protected boolean hasNextPage() {
        return !StringUtils.isBlank(m_nextPage);
    }

    @SuppressWarnings("resource")
    @Override
    protected Iterator<UnityPath> loadNextPage() throws IOException {
        try {
            final CatalogAPI client = m_path.getFileSystem().getCatalogClient();
            final CatalogVolumesInfoList list = client.listVolumes(m_catalog, m_schema, m_nextPage);
            m_nextPage = list.nextPageToken;
            return toIterator(list);
        } catch (final ClientErrorException e) {
            throw toIOException(m_path, e);
        }
    }

    private Iterator<UnityPath> toIterator(final CatalogVolumesInfoList list) {
        final CatalogVolumesInfo[] schemas = list.volumes != null ? list.volumes : new CatalogVolumesInfo[0];
        return Arrays.stream(schemas).map(this::toPath).iterator();
    }

    @SuppressWarnings("resource")
    private UnityPath toPath(final CatalogVolumesInfo volume) {
        final UnityFileSystem fs = m_path.getFileSystem();
        final UnityPath path = new UnityPath(fs, fs.getSeparator(), volume.catalogName, volume.schemaName, volume.name);

        fs.addToAttributeCache(path, UnityFileSystemProvider.createBaseFileAttrs(volume, path));

        return path;
    }

}
