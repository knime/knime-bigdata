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
 *   2024-05-14 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.databricks.rest.catalog;

import java.io.IOException;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;

/**
 * REST API definition to access Unity Catalogs.
 *
 * @see <a href="https://docs.databricks.com/api/workspace/catalogs">Catalogs API</a>
 * @author Sascha Wolke, KNIME GmbH
 */
@Path("2.1/unity-catalog")
public interface CatalogAPI {

    /**
     * @return list of catalogs
     * @throws IOException
     */
    @GET
    @Path("catalogs")
    CatalogInfoList listCatalogs() throws IOException;

    /**
     * @param catalogName name of the catalog
     * @return list of schemas in catalog
     * @throws IOException
     */
    @GET
    @Path("schemas")
    CatalogSchemaInfoList listSchemas(@QueryParam("catalog_name") String catalogName) throws IOException;

    /**
     * @param catalogName name of the catalog
     * @param pageToken optional page token, returned by last listing
     * @return list of schemas in catalog
     * @throws IOException
     */
    @GET
    @Path("schemas")
    CatalogSchemaInfoList listSchemas( //
        @QueryParam("catalog_name") String catalogName, //
        @QueryParam("page_token") String pageToken) throws IOException;

    /**
     * @param catalogName name of the catalog
     * @param schemaName name of the schema
     * @return list of volumes in catalog schema
     * @throws IOException
     */
    @GET
    @Path("volumes")
    CatalogVolumesInfoList listVolumes( //
        @QueryParam("catalog_name") String catalogName, //
        @QueryParam("schema_name") String schemaName) throws IOException;

    /**
     * @param catalogName name of the catalog
     * @param schemaName name of the schema
     * @param pageToken optional page token, returned by last listing
     * @return list of volumes in catalog schema
     * @throws IOException
     */
    @GET
    @Path("volumes")
    CatalogVolumesInfoList listVolumes( //
        @QueryParam("catalog_name") String catalogName, //
        @QueryParam("schema_name") String schemaName, //
        @QueryParam("page_token") String pageToken) throws IOException;


    /**
     * @return metastore assignment of the workspace
     * @throws IOException
     */
    @GET
    @Path("current-metastore-assignment")
    MetastoreAssignmentInfo getMetastoreAssignment() throws IOException;

    /**
     * @return summary of the current metastore
     * @throws IOException
     */
    @GET
    @Path("metastore_summary")
    MetastoreSummary getCurrentMetastore() throws IOException;

    /**
     * @param catalog the name of the catalog
     * @return catalog metadata
     * @throws IOException
     */
    @GET
    @Path("catalogs/{catalog}")
    CatalogInfo getCatalogMetadata(@PathParam("catalog") String catalog) throws IOException;

    /**
     * @param catalog the name of the catalog
     * @param schema the name of the schema
     * @return schema metadata
     * @throws IOException
     */
    @GET
    @Path("schemas/{catalog}.{schema}")
    CatalogSchemaInfo getSchemaMetadata( //
        @PathParam("catalog") String catalog, //
        @PathParam("schema") String schema) throws IOException;

    /**
     * @param catalog the name of the catalog
     * @param schema the name of the schema
     * @param volume the name of the volume
     * @return schema metadata
     * @throws IOException
     */
    @GET
    @Path("volumes/{catalog}.{schema}.{volume}")
    CatalogVolumesInfo getVolumeMetadata( //
        @PathParam("catalog") String catalog, //
        @PathParam("schema") String schema, //
        @PathParam("volume") String volume) throws IOException;

}
