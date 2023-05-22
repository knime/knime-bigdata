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
 *   Nov 12, 2019 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.bigdata.filehandling.knox.rest;

import java.io.IOException;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.web.resources.DeleteOpParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.PostOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;

import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Response;

/**
 * Parts of the <a href="https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html">Apache Hadoop WebHDFS API</a>.
 *
 * @author Sascha Wolke, KNIME GmbH
 * @see <a href="https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html">Apache Hadoop WebHDFS API</a>
 */
@SuppressWarnings("javadoc")
public interface WebHDFSAPI {

    @GET
    @Path("{path}")
    FileStatus getFileStatus(@PathParam("path") String path, @QueryParam("op") GetOpParam.Op op) throws IOException;

    @GET
    @Path("{path}")
    FileStatuses listStatus(@PathParam("path") String path, @QueryParam("op") GetOpParam.Op op) throws IOException;

    @GET
    @Path("{path}")
    ContentSummary getContentSummary(@PathParam("path") String path, @QueryParam("op") GetOpParam.Op op) throws IOException;

    @GET
    @Path("{path}")
    void checkAccess(@PathParam("path") String path, @QueryParam("op") GetOpParam.Op op, @QueryParam("fsaction") String action) throws IOException;

    @GET
    org.apache.hadoop.fs.Path getHomeDirectory(@QueryParam("op") GetOpParam.Op op) throws IOException;

    @PUT
    @Path("{path}")
    boolean mkdirs(@PathParam("path") String path, @QueryParam("op") PutOpParam.Op op) throws IOException;

    @PUT
    @Path("{path}")
    boolean rename(@PathParam("path") String path, @QueryParam("op") PutOpParam.Op op,
        @QueryParam("destination") String destination) throws IOException;

    @PUT
    @Path("{path}")
    void setPermission(@PathParam("path") String path, @QueryParam("op") PutOpParam.Op op,
        @QueryParam("permission") short permission) throws IOException;

    default void setPermission(final String path, final FsPermission permission) throws IOException {
        final int n = permission.toShort();
        final int octal = ((n >>> 9) & 1) * 1000 + ((n >>> 6) & 7) * 100 + ((n >>> 3) & 7) * 10 + (n & 7);
        setPermission(path, PutOpParam.Op.SETPERMISSION, (short)octal);
    }

    @DELETE
    @Path("{path}")
    boolean delete(@PathParam("path") String path, @QueryParam("op") DeleteOpParam.Op op,
        @QueryParam("recursive") boolean recursive) throws IOException;

    @GET
    @Path("{path}")
    Response open(@PathParam("path") String path, @QueryParam("op") GetOpParam.Op op,
        @QueryParam("buffersize") long buffersize);

    @PUT
    @Path("{path}")
    Response create(@PathParam("path") String path, @QueryParam("op") PutOpParam.Op op,
        @QueryParam("buffersize") long buffersize, @QueryParam("overwrite") boolean overwrite);

    @POST
    @Path("{path}")
    Response append(@PathParam("path") String path, @QueryParam("op") PostOpParam.Op op,
        @QueryParam("buffersize") long buffersize);

    @PUT
    @Path("{path")
    Response setTimes(@PathParam("path") String path, @QueryParam("op") PutOpParam.Op op,
        @QueryParam("modificationtime") long mtime, @QueryParam("accesstime") long atime) throws IOException;

    @PUT
    @Path("{path")
    Response setOwner(@PathParam("path") String path, @QueryParam("op") PutOpParam.Op op,
        @QueryParam("owner") String user) throws IOException;

    @PUT
    @Path("{path")
    Response setGroup(@PathParam("path") String path, @QueryParam("op") PutOpParam.Op op,
        @QueryParam("group") String group) throws IOException;

}
