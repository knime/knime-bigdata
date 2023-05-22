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
 *   Created on Jul 18, 2019 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.databricks.rest.dbfs;

import java.io.IOException;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;

/**
 * REST API definition of DBFS.
 *
 * @see <a href="https://docs.databricks.com/api/latest/dbfs.html#dbfs-api">DBFS API</a>
 * @author Sascha Wolke, KNIME GmbH
 */
@Path("2.0/dbfs")
public interface DBFSAPI {

    /**
     * Get the file information of a file or directory.
     *
     * @param path The path of the file or directory
     * @return file informations
     * @throws IOException on failures
     */
    @GET
    @Path("get-status")
    FileInfo getStatus(@QueryParam("path") String path) throws IOException;

    /**
     * List the contents of a directory, or details of the file.
     *
     * @param path The path of the file or directory
     * @return list of file informations
     * @throws IOException on failures
     */
    @GET
    @Path("list")
    FileInfoList list(@QueryParam("path") String path) throws IOException;

    /**
     * Create the given directory and necessary parent directories if they do not exist.
     *
     * @param mkdir Make directory request with the path of the new directory
     * @throws IOException on failures
     */
    @POST
    @Path("mkdirs")
    void mkdirs(Mkdir mkdir) throws IOException;

    /**
     * Move a file from one location to another location within DBFS.
     *
     * @param move Move request with source and destination path.
     * @throws IOException on failures
     */
    @POST
    @Path("move")
    void move(Move move) throws IOException;

    /**
     * Delete the file or directory (optionally recursively delete all files in the directory).
     *
     * @param delete Delete request with the path to delete.
     * @throws IOException on failures
     */
    @POST
    @Path("delete")
    void delete(Delete delete) throws IOException;

    /**
     * Return the contents of a file.
     *
     * @param path The path of the file to read. The path should be the absolute DBFS path (e.g. /mnt/foo/).
     * @param offset The offset to read from in bytes.
     * @param length The number of bytes to read starting from the offset. This has a limit of 1 MB, and a
     *        default value of 0.5 MB.
     * @return {@link FileBlock}
     * @throws IOException on failures
     */
    @GET
    @Path("read")
    FileBlock read(@QueryParam("path") String path, @QueryParam("offset") long offset,
        @QueryParam("length") long length) throws IOException;

    /**
     * Open a stream to write to a file and returns a handle to this stream. There is a 10 minute idle timeout on this
     * handle.
     *
     * @param create Create request
     * @return file handle
     * @throws IOException on failures
     */
    @POST
    @Path("create")
    FileHandle create(Create create) throws IOException;

    /**
     * Append a block of data to the stream specified by the input handle.
     *
     * @param addBlock {@link AddBlock} request
     * @throws IOException on failures
     */
    @POST
    @Path("add-block")
    void addBlock(AddBlock addBlock) throws IOException;

    /**
     * Close the stream specified by the input handle.
     *
     * @param close {@link Close} request
     * @throws IOException on failures
     */
    @POST
    @Path("close")
    void close(Close close) throws IOException;
}
