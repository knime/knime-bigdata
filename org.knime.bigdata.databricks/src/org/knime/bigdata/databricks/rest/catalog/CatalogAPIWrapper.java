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
package org.knime.bigdata.databricks.rest.catalog;

import java.io.IOException;

import org.knime.bigdata.databricks.rest.APIWrapper;

/**
 * Wrapper class for {@link CatalogAPI} that suppresses authentication popups.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
public class CatalogAPIWrapper extends APIWrapper<CatalogAPI> implements CatalogAPI {

    /**
     * Default constructor.
     *
     * @param api the api to wrap
     */
    public CatalogAPIWrapper(final CatalogAPI api) {
        super(api);
    }

    @Override
    public CatalogInfoList listCatalogs() throws IOException {
        return invoke(m_api::listCatalogs);
    }

    @Override
    public CatalogSchemaInfoList listSchemas(final String catalogName) throws IOException {
        return invoke(() -> m_api.listSchemas(catalogName));
    }

    @Override
    public CatalogSchemaInfoList listSchemas(final String catalogName, final String pageToken) throws IOException {
        return invoke(() -> m_api.listSchemas(catalogName, pageToken));
    }

    @Override
    public CatalogVolumesInfoList listVolumes(final String catalogName, final String schemaName) throws IOException {
        return invoke(() -> m_api.listVolumes(catalogName, schemaName));
    }

    @Override
    public CatalogVolumesInfoList listVolumes(final String catalogName, final String schemaName, final String pageToken)
        throws IOException {
        return invoke(() -> m_api.listVolumes(catalogName, schemaName, pageToken));
    }

    @Override
    public MetastoreAssignmentInfo getMetastoreAssignment() throws IOException {
        return invoke(m_api::getMetastoreAssignment);
    }

    @Override
    public MetastoreSummary getCurrentMetastore() throws IOException {
        return invoke(m_api::getCurrentMetastore);
    }

    @Override
    public CatalogInfo getCatalogMetadata(final String catalog) throws IOException {
        return invoke(() -> m_api.getCatalogMetadata(catalog));
    }

    @Override
    public CatalogSchemaInfo getSchemaMetadata(final String catalog, final String schema) throws IOException {
        return invoke(() -> m_api.getSchemaMetadata(catalog, schema));
    }

    @Override
    public CatalogVolumesInfo getVolumeMetadata(final String catalog, final String schema, final String volume)
        throws IOException {
        return invoke(() -> m_api.getVolumeMetadata(catalog, schema, volume));
    }

}
