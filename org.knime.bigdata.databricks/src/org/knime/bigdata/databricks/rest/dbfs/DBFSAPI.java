/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Jul 18, 2019 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.databricks.rest.dbfs;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * REST API definition of DBFS.
 *
 * @see <a href="https://docs.databricks.com/api/latest/dbfs.html#dbfs-api">DBFS API</a>
 * @author Sascha Wolke, KNIME GmbH
 */
@Path("dbfs")
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
    FileInfoReponse getStatus(@QueryParam("path") String path) throws IOException;

    /**
     * List the contents of a directory, or details of the file.
     *
     * @param path The path of the file or directory
     * @return list of file informations
     * @throws IOException on failures
     */
    @GET
    @Path("list")
    FileInfoListResponse list(@QueryParam("path") String path) throws IOException;

    /**
     * Create the given directory and necessary parent directories if they do not exist.
     *
     * @param mkdir Make directory request with the path of the new directory
     * @throws IOException on failures
     */
    @POST
    @Path("mkdirs")
    void mkdirs(MkdirRequest mkdir) throws IOException;

    /**
     * Move a file from one location to another location within DBFS.
     *
     * @param move Move request with source and destination path.
     * @throws IOException on failures
     */
    @POST
    @Path("move")
    void move(MoveRequest move) throws IOException;

    /**
     * Delete the file or directory (optionally recursively delete all files in the directory).
     *
     * @param delete Delete request with the path to delete.
     * @throws IOException on failures
     */
    @POST
    @Path("delete")
    void delete(DeleteRequest delete) throws IOException;

    /**
     * Return the contents of a file.
     *
     * @param path The path of the file to read. The path should be the absolute DBFS path (e.g. /mnt/foo/).
     * @param offset The offset to read from in bytes.
     * @param length The number of bytes to read starting from the offset. This has a limit of 1 MB, and a
     *        default value of 0.5 MB.
     * @return {@link FileBlockResponse}
     * @throws IOException on failures
     */
    @GET
    @Path("read")
    FileBlockResponse read(@QueryParam("path") String path, @QueryParam("offset") long offset,
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
    FileHandleResponse create(CreateRequest create) throws IOException;

    /**
     * Append a block of data to the stream specified by the input handle.
     *
     * @param addBlock {@link AddBlockRequest} request
     * @throws IOException on failures
     */
    @POST
    @Path("add-block")
    void addBlock(AddBlockRequest addBlock) throws IOException;

    /**
     * Close the stream specified by the input handle.
     *
     * @param close {@link CloseRequest} request
     * @throws IOException on failures
     */
    @POST
    @Path("close")
    void close(CloseRequest close) throws IOException;
}
