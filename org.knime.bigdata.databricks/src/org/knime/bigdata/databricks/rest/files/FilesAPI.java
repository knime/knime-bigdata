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
package org.knime.bigdata.databricks.rest.files;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Response;

/**
 * REST API definition of files API.
 *
 * @see <a href="https://docs.databricks.com/api/workspace/files">Files API</a>
 * @author Sascha Wolke, KNIME GmbH
 */
@Path("2.0/fs")
public interface FilesAPI {

    /**
     * Get metadata of a directory in a volume.
     *
     * @param path absolute path of the directory without the {@code /Volumes} prefix, but with catalog, schema and
     *            volume name
     * @throws FileNotFoundException if the given path is a file or the path does not exist
     * @throws IOException
     */
    @HEAD
    @Path("directories/Volumes{path}")
    void getDirectoryMetadata(@PathParam("path") String path) throws IOException;

    /**
     * Get metadata of a file in a volume.
     *
     * @param path absolute path of the file without the {@code /Volumes} prefix, but with catalog, schema and volume
     *            name
     * @return response with metadata headers
     * @throws FileNotFoundException if the given path is a file or the path does not exist
     * @throws IOException
     */
    @HEAD
    @Path("files/Volumes{path}")
    Response getFileMetadata(@PathParam("path") String path) throws IOException;

    /**
     * List the contents of a directory in a volume.
     *
     * @param path absolute path of the directory without the {@code /Volumes} prefix, but with catalog, schema and
     *            volume name
     * @return list of files and directories
     * @throws IOException
     */
    @GET
    @Path("directories/Volumes{path}")
    FileInfoList list(@PathParam("path") String path) throws IOException;

    /**
     * List the contents of a directory in a volume.
     *
     * @param path absolute path of the directory without the {@code /Volumes} prefix, but with catalog, schema and
     *            volume name
     * @param pageToken optional page token, returned by last listing
     * @return list of files and directories
     * @throws IOException
     */
    @GET
    @Path("directories/Volumes{path}")
    FileInfoList list(@PathParam("path") String path, @QueryParam("page_token") String pageToken) throws IOException;

    /**
     * Create the given directory in a volume, and necessary parent directories if they do not exist.
     *
     * @param path absolute path of the directory without the {@code /Volumes} prefix, but with catalog, schema and
     *            volume name
     * @throws IOException
     */
    @PUT
    @Path("directories/Volumes{path}")
    void mkdirs(@PathParam("path") String path) throws IOException;

    /**
     * Delete an empty directory in a volume.
     *
     * @param path absolute path of the directory without the {@code /Volumes} prefix, but with catalog, schema and
     *            volume name
     * @throws IOException
     */
    @DELETE
    @Path("directories/Volumes{path}")
    void deleteDirectory(@PathParam("path") String path) throws IOException;

    /**
     * Delete a file in a volume.
     *
     * @param path absolute path of the file without the {@code /Volumes} prefix, but with catalog, schema and volume
     *            name
     * @throws IOException
     */
    @DELETE
    @Path("files/Volumes{path}")
    void deleteFile(@PathParam("path") String path) throws IOException;

    /**
     * Download a file in a volume.
     *
     * @param path absolute path of the file without the {@code /Volumes} prefix, but with catalog, schema and volume
     *            name
     * @return input stream of file contents
     * @throws IOException
     */
    @GET
    @Path("files/Volumes{path}")
    InputStream download(@PathParam("path") String path) throws IOException;

    /**
     * Upload up to 5GiB to a file in a volume.
     *
     * @param path absolute path of the file without the {@code /Volumes} prefix, but with catalog, schema and volume
     *            name
     * @param content input stream of the file contents
     * @param overwrite if {@code true}, existing file will be overwritten
     * @throws IOException
     */
    @PUT
    @Path("files/Volumes{path}")
    void upload(@PathParam("path") String path, @QueryParam("overwrite") boolean overwrite, InputStream content)
        throws IOException;

}
