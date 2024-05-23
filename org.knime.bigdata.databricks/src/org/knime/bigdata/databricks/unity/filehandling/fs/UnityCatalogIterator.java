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

import org.knime.bigdata.databricks.rest.catalog.CatalogAPI;
import org.knime.bigdata.databricks.rest.catalog.CatalogInfo;
import org.knime.bigdata.databricks.rest.catalog.CatalogInfoList;
import org.knime.filehandling.core.connections.base.BasePathIterator;

import jakarta.ws.rs.ClientErrorException;

/**
 * Iterator to list Unity Catalogs.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
public class UnityCatalogIterator extends BasePathIterator<UnityPath> {

    @SuppressWarnings("resource")
    UnityCatalogIterator(final UnityPath path, final Filter<? super Path> filter) throws IOException {
        super(path, filter);

        if (!path.isRoot()) {
            throw new IOException("Only root supported in catalog listing");
        }

        try {
            final CatalogAPI client = m_path.getFileSystem().getCatalogClient();
            final CatalogInfoList list = client.listCatalogs();
            setFirstPage(toIterator(list)); // NOSONAR
        } catch (final ClientErrorException e) {
            throw toIOException(path, e);
        }
    }

    private Iterator<UnityPath> toIterator(final CatalogInfoList list) {
        final CatalogInfo[] files = list.catalogs != null ? list.catalogs : new CatalogInfo[0];
        return Arrays.stream(files).map(this::toPath).iterator();
    }

    @SuppressWarnings("resource")
    private UnityPath toPath(final CatalogInfo catalog) {
        final UnityFileSystem fs = m_path.getFileSystem();
        final UnityPath path = new UnityPath(fs, fs.getSeparator(), catalog.name);

        fs.addToAttributeCache(path, UnityFileSystemProvider.createBaseFileAttrs(catalog, path));

        return path;
    }

}
